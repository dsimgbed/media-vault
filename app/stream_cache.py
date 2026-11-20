from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
import hashlib
import logging
import os
from pathlib import Path
import re
import threading
import time
from typing import Any
import uuid

logger = logging.getLogger(__name__)

_BLK_FILENAME_RE = re.compile(r"^([0-9a-f]{24})_([0-9a-f]{16})(?:_([0-9a-f]{8}))?\.blk$")
_UTIME_THROTTLE_SEC = 60.0
_MAX_PREFETCHED_KEYS = 1024
_MAX_SEQ_NAMESPACES = 256
_SEQ_GAP_TOLERANCE_BYTES = 4096


class TwoTierBlockCache:
    """Thread-safe two-tier (RAM L1 + optional persistent Disk L2) LRU block cache.

    Lock hierarchy invariant (deadlock-free, non-blocking):
        * ``self._lock`` is a short in-memory leaf lock protecting L1 state and
          aggregate hit/miss counters. It NEVER performs disk I/O and NEVER
          acquires ``self._disk_lock``.
        * ``self._disk_lock`` is a short in-memory lock protecting L2 index
          metadata and counters ONLY. It NEVER wraps blocking disk I/O
          (reads, writes, ``os.walk``, ``unlink``, ``os.utime``) and NEVER
          acquires ``self._lock`` while held.
    """

    def __init__(
        self,
        max_bytes: int,
        block_size: int = 512 * 1024,
        *,
        disk_max_bytes: int = 0,
        disk_dir: Path | str | None = None,
    ) -> None:
        self._max_bytes = max(0, int(max_bytes))
        self._block_size = max(4096, int(block_size))
        self._disk_max_bytes = max(0, int(disk_max_bytes))
        self._disk_dir = self._normalize_disk_dir(disk_dir)

        # L1 RAM state (guarded by self._lock)
        self._entries: OrderedDict[tuple[Any, int], bytes] = OrderedDict()
        self._index: dict[Any, dict[int, int]] = {}
        self._total_bytes = 0
        self._l1_hits = 0
        self._l1_misses = 0
        self._l1_evictions = 0
        self._total_hits = 0
        self._total_misses = 0
        self._lock = threading.RLock()

        # L2 Disk state (guarded by self._disk_lock)
        self._l2_block_size = self._block_size
        self._l2_entries: OrderedDict[tuple[str, int], tuple[Path, int]] = OrderedDict()
        self._l2_ns_index: dict[str, dict[int, int]] = {}
        self._l2_total_bytes = 0
        self._l2_hits = 0
        self._l2_misses = 0
        self._l2_evictions = 0
        self._l2_last_touch: dict[tuple[str, int], float] = {}
        self._disk_lock = threading.RLock()

        if self.l2_enabled:
            self._reconcile_disk_cache()

    @staticmethod
    def _normalize_disk_dir(disk_dir: Path | str | None) -> Path | None:
        if disk_dir is None:
            return None
        s = str(disk_dir).strip()
        if not s:
            return None
        return Path(disk_dir)

    @property
    def enabled(self) -> bool:
        return self._max_bytes > 0 or (self._disk_max_bytes > 0 and self._disk_dir is not None)

    @property
    def l1_enabled(self) -> bool:
        return self._max_bytes > 0

    @property
    def l2_enabled(self) -> bool:
        return self._disk_max_bytes > 0 and self._disk_dir is not None

    @property
    def max_bytes(self) -> int:
        return self._max_bytes

    @property
    def disk_max_bytes(self) -> int:
        return self._disk_max_bytes

    @property
    def disk_dir(self) -> Path | None:
        return self._disk_dir

    @property
    def block_size(self) -> int:
        return self._block_size

    @property
    def total_bytes(self) -> int:
        with self._lock:
            return self._total_bytes

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    @staticmethod
    def _is_persistent_ns(cache_ns: Any) -> bool:
        return (
            isinstance(cache_ns, tuple)
            and len(cache_ns) == 2
            and isinstance(cache_ns[0], str)
            and bool(cache_ns[0])
            and cache_ns[0] != "media"
            and isinstance(cache_ns[1], int)
            and not isinstance(cache_ns[1], bool)
        )

    @staticmethod
    def _ns_hash(cache_ns: tuple[str, int]) -> str:
        raw = f"{cache_ns[0]}:{cache_ns[1]}".encode("utf-8")
        return hashlib.sha256(raw).hexdigest()[:24]

    @staticmethod
    def _unlink_paths(paths: list[Path]) -> None:
        for p in paths:
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass

    def configure(
        self,
        max_bytes: int,
        block_size: int,
        *,
        disk_max_bytes: int | None = None,
        disk_dir: Path | str | None = None,
    ) -> None:
        max_bytes = max(0, int(max_bytes))
        block_size = max(4096, int(block_size))
        with self._lock:
            block_size_changed = block_size != self._block_size
            self._max_bytes = max_bytes
            self._block_size = block_size
            if max_bytes <= 0 or block_size_changed:
                self._entries.clear()
                self._index.clear()
                self._total_bytes = 0
            else:
                self._evict_l1_to_limit()

        if disk_max_bytes is not None or disk_dir is not None or block_size_changed:
            need_reconcile = False
            evicted_paths: list[Path] = []
            with self._disk_lock:
                old_enabled = self.l2_enabled
                old_dir = self._disk_dir
                old_max = self._disk_max_bytes
                old_blk = self._l2_block_size

                if disk_max_bytes is not None:
                    self._disk_max_bytes = max(0, int(disk_max_bytes))
                if disk_dir is not None:
                    self._disk_dir = self._normalize_disk_dir(disk_dir)
                self._l2_block_size = block_size

                new_enabled = self.l2_enabled
                if not new_enabled:
                    self._l2_entries.clear()
                    self._l2_ns_index.clear()
                    self._l2_total_bytes = 0
                    self._l2_last_touch.clear()
                elif not old_enabled or self._disk_dir != old_dir or self._l2_block_size != old_blk:
                    need_reconcile = True
                elif self._disk_max_bytes < old_max:
                    evicted_paths = self._evict_l2_to_limit_locked()

            if need_reconcile:
                self._reconcile_disk_cache()
            elif evicted_paths:
                self._unlink_paths(evicted_paths)

    def _evict_l1_to_limit(self) -> None:
        while self._total_bytes > self._max_bytes and self._entries:
            evicted_key, evicted_data = self._entries.popitem(last=False)
            self._total_bytes -= len(evicted_data)
            self._l1_evictions += 1
            ev_ns, ev_off = evicted_key
            ns_map = self._index.get(ev_ns)
            if ns_map is not None:
                ns_map.pop(ev_off, None)
                if not ns_map:
                    self._index.pop(ev_ns, None)

    def _evict_l2_to_limit_locked(self) -> list[Path]:
        evicted_paths: list[Path] = []
        while self._l2_total_bytes > self._disk_max_bytes and self._l2_entries:
            (ev_ns_hash, ev_off), (ev_path, ev_size) = self._l2_entries.popitem(last=False)
            self._l2_total_bytes = max(0, self._l2_total_bytes - ev_size)
            self._l2_evictions += 1
            self._l2_last_touch.pop((ev_ns_hash, ev_off), None)
            ns_map = self._l2_ns_index.get(ev_ns_hash)
            if ns_map is not None:
                ns_map.pop(ev_off, None)
                if not ns_map:
                    self._l2_ns_index.pop(ev_ns_hash, None)
            evicted_paths.append(ev_path)
        if not self._l2_entries:
            self._l2_total_bytes = 0
        return evicted_paths

    def _reconcile_disk_cache(self) -> None:
        with self._disk_lock:
            if not self.l2_enabled or self._disk_dir is None:
                self._l2_entries.clear()
                self._l2_ns_index.clear()
                self._l2_total_bytes = 0
                self._l2_last_touch.clear()
                return
            target_dir = self._disk_dir
            target_block_size = self._l2_block_size

        try:
            target_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            logger.warning("Failed to create L2 disk cache dir %s: %s", target_dir, exc)
            return

        found: list[tuple[int, str, int, Path, int]] = []
        for root, dirs, files in os.walk(target_dir, followlinks=False):
            root_path = Path(root)
            dirs[:] = [d for d in dirs if not (root_path / d).is_symlink()]
            for fname in files:
                fpath = root_path / fname
                if fname.startswith(".tmp_"):
                    try:
                        if not fpath.is_symlink():
                            fpath.unlink(missing_ok=True)
                    except OSError:
                        pass
                    continue
                match = _BLK_FILENAME_RE.match(fname)
                if not match:
                    continue
                try:
                    if fpath.is_symlink() or not fpath.is_file():
                        continue
                    st = fpath.stat()
                except OSError:
                    continue
                size = st.st_size
                blk_hex = match.group(3)
                file_blk_size = int(blk_hex, 16) if blk_hex is not None else None
                if size <= 0 or file_blk_size != target_block_size:
                    try:
                        fpath.unlink(missing_ok=True)
                    except OSError:
                        pass
                    continue
                ns_hash = match.group(1)
                offset = int(match.group(2), 16)
                found.append((st.st_mtime_ns, ns_hash, offset, fpath, size))

        found.sort(key=lambda item: item[0])
        evicted_paths: list[Path] = []
        with self._disk_lock:
            if not self.l2_enabled or self._disk_dir != target_dir or self._l2_block_size != target_block_size:
                return
            self._l2_entries.clear()
            self._l2_ns_index.clear()
            self._l2_total_bytes = 0
            self._l2_last_touch.clear()
            for _mtime_ns, ns_hash, offset, fpath, size in found:
                l2_key = (ns_hash, offset)
                if l2_key in self._l2_entries:
                    _, old_size = self._l2_entries.pop(l2_key)
                    self._l2_total_bytes = max(0, self._l2_total_bytes - old_size)
                self._l2_entries[l2_key] = (fpath, size)
                self._l2_ns_index.setdefault(ns_hash, {})[offset] = size
                self._l2_total_bytes += size
            evicted_paths = self._evict_l2_to_limit_locked()

        if evicted_paths:
            self._unlink_paths(evicted_paths)

    def _put_l1(self, cache_ns: Any, off: int, data: bytes) -> None:
        if self._max_bytes <= 0 or not data or len(data) > self._max_bytes:
            return
        key = (cache_ns, off)
        with self._lock:
            existing = self._entries.get(key)
            if existing is not None:
                if len(data) <= len(existing):
                    self._entries.move_to_end(key)
                    return
                self._total_bytes -= len(existing)
            self._entries[key] = data
            self._entries.move_to_end(key)
            self._index.setdefault(cache_ns, {})[off] = len(data)
            self._total_bytes += len(data)
            self._evict_l1_to_limit()

    def _should_touch_utime_locked(self, l2_key: tuple[str, int]) -> bool:
        now = time.monotonic()
        last = self._l2_last_touch.get(l2_key)
        if last is None or (now - last) >= _UTIME_THROTTLE_SEC:
            self._l2_last_touch[l2_key] = now
            return True
        return False

    def _put_l2_sync(self, cache_ns: tuple[str, int], off: int, data: bytes) -> None:
        ns_hash = self._ns_hash(cache_ns)
        l2_key = (ns_hash, off)
        with self._disk_lock:
            if not self.l2_enabled or self._disk_dir is None or len(data) > self._disk_max_bytes:
                return
            disk_dir = self._disk_dir
            blk_size = self._l2_block_size
            existing = self._l2_entries.get(l2_key)
            existing_path: Path | None = None
            existing_size = 0
            if existing is not None:
                existing_path, existing_size = existing

        if existing_path is not None and len(data) <= existing_size and existing_path.exists():
            need_utime = False
            with self._disk_lock:
                cur = self._l2_entries.get(l2_key)
                if cur is not None and cur[0] == existing_path:
                    self._l2_entries.move_to_end(l2_key)
                    need_utime = self._should_touch_utime_locked(l2_key)
            if need_utime:
                try:
                    os.utime(existing_path, None)
                except OSError:
                    pass
            return

        shard_dir = disk_dir / ns_hash[:2]
        final_path = shard_dir / f"{ns_hash}_{off:016x}_{blk_size:08x}.blk"
        tmp_path = shard_dir / f".tmp_{uuid.uuid4().hex}.blk"
        try:
            shard_dir.mkdir(parents=True, exist_ok=True)
            with open(tmp_path, "wb") as fh:
                fh.write(data)
                fh.flush()
            os.replace(tmp_path, final_path)
        except OSError as exc:
            logger.warning("Failed to write L2 cache block %s: %s", final_path, exc)
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
            return

        evicted_paths: list[Path] = []
        stale_write = False
        with self._disk_lock:
            if not self.l2_enabled or self._disk_dir != disk_dir or self._l2_block_size != blk_size or len(data) > self._disk_max_bytes:
                stale_write = True
            else:
                prev = self._l2_entries.get(l2_key)
                if prev is not None:
                    self._l2_total_bytes = max(0, self._l2_total_bytes - prev[1])
                self._l2_entries[l2_key] = (final_path, len(data))
                self._l2_entries.move_to_end(l2_key)
                self._l2_ns_index.setdefault(ns_hash, {})[off] = len(data)
                self._l2_total_bytes += len(data)
                self._l2_last_touch[l2_key] = time.monotonic()
                evicted_paths = self._evict_l2_to_limit_locked()

        if stale_write:
            self._unlink_paths([final_path])
        elif evicted_paths:
            self._unlink_paths(evicted_paths)

    def _remove_stale_l2_entry_locked(
        self, ns_hash: str, seg_start: int, expected_path: Path | None = None
    ) -> None:
        l2_key = (ns_hash, seg_start)
        cur = self._l2_entries.get(l2_key)
        if cur is None:
            return
        if expected_path is not None and cur[0] != expected_path:
            return
        self._l2_entries.pop(l2_key, None)
        self._l2_last_touch.pop(l2_key, None)
        self._l2_total_bytes = max(0, self._l2_total_bytes - cur[1])
        ns_map = self._l2_ns_index.get(ns_hash)
        if ns_map is not None:
            ns_map.pop(seg_start, None)
            if not ns_map:
                self._l2_ns_index.pop(ns_hash, None)

    def _read_l2_span_sync(self, ns_hash: str, cursor: int) -> tuple[int, bytes] | None:
        with self._disk_lock:
            if not self.l2_enabled:
                return None
            segs = self._l2_ns_index.get(ns_hash)
            if not segs:
                return None
            best_start: int | None = None
            best_reach = cursor
            for seg_start, seg_len in segs.items():
                seg_end = seg_start + seg_len
                if seg_start <= cursor < seg_end and seg_end > best_reach:
                    best_start = seg_start
                    best_reach = seg_end
            if best_start is None:
                return None
            l2_key = (ns_hash, best_start)
            entry = self._l2_entries.get(l2_key)
            if entry is None:
                return None
            path, expected_size = entry

        try:
            data = path.read_bytes()
        except OSError:
            with self._disk_lock:
                self._remove_stale_l2_entry_locked(ns_hash, best_start, path)
            return None

        if not data:
            with self._disk_lock:
                self._remove_stale_l2_entry_locked(ns_hash, best_start, path)
            return None

        need_utime = False
        with self._disk_lock:
            cur = self._l2_entries.get(l2_key)
            if cur is not None and cur[0] == path:
                if len(data) != cur[1]:
                    self._l2_total_bytes = max(0, self._l2_total_bytes - cur[1]) + len(data)
                    self._l2_entries[l2_key] = (path, len(data))
                    ns_map = self._l2_ns_index.get(ns_hash)
                    if ns_map is not None:
                        ns_map[best_start] = len(data)
                if not (best_start <= cursor < best_start + len(data)):
                    return None
                self._l2_entries.move_to_end(l2_key)
                need_utime = self._should_touch_utime_locked(l2_key)
            elif not (best_start <= cursor < best_start + len(data)):
                return None
            self._l2_hits += 1

        if need_utime:
            try:
                os.utime(path, None)
            except OSError:
                pass
        return best_start, data

    def _read_l2_exact_sync(self, ns_hash: str, block_offset: int) -> bytes | None:
        l2_key = (ns_hash, block_offset)
        with self._disk_lock:
            if not self.l2_enabled:
                return None
            entry = self._l2_entries.get(l2_key)
            if entry is None:
                return None
            path, _expected_size = entry

        try:
            data = path.read_bytes()
        except OSError:
            with self._disk_lock:
                self._remove_stale_l2_entry_locked(ns_hash, block_offset, path)
            return None

        if not data:
            with self._disk_lock:
                self._remove_stale_l2_entry_locked(ns_hash, block_offset, path)
            return None

        need_utime = False
        with self._disk_lock:
            cur = self._l2_entries.get(l2_key)
            if cur is not None and cur[0] == path:
                if len(data) != cur[1]:
                    self._l2_total_bytes = max(0, self._l2_total_bytes - cur[1]) + len(data)
                    self._l2_entries[l2_key] = (path, len(data))
                    ns_map = self._l2_ns_index.get(ns_hash)
                    if ns_map is not None:
                        ns_map[block_offset] = len(data)
                self._l2_entries.move_to_end(l2_key)
                need_utime = self._should_touch_utime_locked(l2_key)
            self._l2_hits += 1

        if need_utime:
            try:
                os.utime(path, None)
            except OSError:
                pass
        return data

    def get_span(self, cache_ns: Any, cursor: int, end: int) -> bytes | None:
        if not self.enabled or cursor > end:
            return None

        l1_checked = False
        with self._lock:
            if self._max_bytes > 0:
                l1_checked = True
                segs = self._index.get(cache_ns)
                if segs:
                    best_start: int | None = None
                    best_reach = cursor
                    for seg_start, seg_len in segs.items():
                        seg_end = seg_start + seg_len
                        if seg_start <= cursor < seg_end and seg_end > best_reach:
                            best_start = seg_start
                            best_reach = seg_end
                    if best_start is not None:
                        key = (cache_ns, best_start)
                        data = self._entries.get(key)
                        if data is not None:
                            rel_start = cursor - best_start
                            rel_end = min(len(data), end - best_start + 1)
                            if rel_end > rel_start:
                                self._entries.move_to_end(key)
                                self._l1_hits += 1
                                self._total_hits += 1
                                return data[rel_start:rel_end]

        l2_eligible = self.l2_enabled and self._is_persistent_ns(cache_ns)
        if l2_eligible:
            ns_hash = self._ns_hash(cache_ns)
            l2_res = self._read_l2_span_sync(ns_hash, cursor)
            if l2_res is not None:
                l2_start, l2_data = l2_res
                rel_start = cursor - l2_start
                rel_end = min(len(l2_data), end - l2_start + 1)
                if rel_end > rel_start:
                    if self._max_bytes > 0:
                        self._put_l1(cache_ns, l2_start, l2_data)
                    with self._lock:
                        if l1_checked:
                            self._l1_misses += 1
                        self._total_hits += 1
                    return l2_data[rel_start:rel_end]

        with self._lock:
            if l1_checked:
                self._l1_misses += 1
            self._total_misses += 1
        if self.l2_enabled:
            with self._disk_lock:
                self._l2_misses += 1
        return None

    async def get_block_async(self, cache_ns: Any, block_offset: int) -> tuple[bytes | None, str]:
        if not self.enabled:
            return None, "miss"
        off = int(block_offset)
        if off < 0:
            return None, "miss"

        l1_checked = False
        with self._lock:
            if self._max_bytes > 0:
                l1_checked = True
                key = (cache_ns, off)
                data = self._entries.get(key)
                if data is not None:
                    self._entries.move_to_end(key)
                    self._l1_hits += 1
                    self._total_hits += 1
                    return data, "l1"

        if self.l2_enabled and self._is_persistent_ns(cache_ns):
            ns_hash = self._ns_hash(cache_ns)
            in_l2 = False
            with self._disk_lock:
                in_l2 = (ns_hash, off) in self._l2_entries
            if in_l2:
                l2_data = await asyncio.to_thread(self._read_l2_exact_sync, ns_hash, off)
                if l2_data is not None:
                    if self._max_bytes > 0:
                        self._put_l1(cache_ns, off, l2_data)
                    with self._lock:
                        if l1_checked:
                            self._l1_misses += 1
                        self._total_hits += 1
                    return l2_data, "l2"

        with self._lock:
            if l1_checked:
                self._l1_misses += 1
            self._total_misses += 1
        if self.l2_enabled:
            with self._disk_lock:
                self._l2_misses += 1
        return None, "miss"

    def put(self, cache_ns: Any, offset: int, data: bytes) -> None:
        if not data:
            return
        off = int(offset)
        if off < 0:
            return
        if self._max_bytes > 0 and len(data) <= self._max_bytes:
            self._put_l1(cache_ns, off, data)
        if self.l2_enabled and self._is_persistent_ns(cache_ns) and len(data) <= self._disk_max_bytes:
            self._put_l2_sync(cache_ns, off, data)

    async def put_block_async(self, cache_ns: Any, block_offset: int, data: bytes) -> None:
        if not data:
            return
        off = int(block_offset)
        if off < 0:
            return
        if self._max_bytes > 0 and len(data) <= self._max_bytes:
            self._put_l1(cache_ns, off, data)
        if self.l2_enabled and self._is_persistent_ns(cache_ns) and 0 < len(data) <= self._disk_max_bytes:
            await asyncio.to_thread(self._put_l2_sync, cache_ns, off, data)

    def has_block(self, cache_ns: Any, block_offset: int) -> bool:
        off = int(block_offset)
        if off < 0 or not self.enabled:
            return False
        with self._lock:
            if self._max_bytes > 0 and (cache_ns, off) in self._entries:
                return True
        if self.l2_enabled and self._is_persistent_ns(cache_ns):
            ns_hash = self._ns_hash(cache_ns)
            with self._disk_lock:
                if (ns_hash, off) in self._l2_entries:
                    return True
        return False

    def invalidate(self, cache_ns: Any) -> int:
        removed_l1 = 0
        with self._lock:
            segs = self._index.pop(cache_ns, None)
            if segs:
                for seg_start in segs:
                    evicted = self._entries.pop((cache_ns, seg_start), None)
                    if evicted is not None:
                        self._total_bytes -= len(evicted)
                        removed_l1 += 1

        removed_l2 = 0
        paths_to_unlink: list[Path] = []
        if self._is_persistent_ns(cache_ns):
            ns_hash = self._ns_hash(cache_ns)
            with self._disk_lock:
                l2_segs = self._l2_ns_index.pop(ns_hash, None)
                if l2_segs:
                    for seg_start in l2_segs:
                        l2_key = (ns_hash, seg_start)
                        self._l2_last_touch.pop(l2_key, None)
                        evicted = self._l2_entries.pop(l2_key, None)
                        if evicted is not None:
                            path, size = evicted
                            self._l2_total_bytes = max(0, self._l2_total_bytes - size)
                            removed_l2 += 1
                            paths_to_unlink.append(path)

        if paths_to_unlink:
            self._unlink_paths(paths_to_unlink)

        return removed_l1 + removed_l2

    def purge(self, *, clear_ram: bool = True, clear_disk: bool = True) -> dict[str, int]:
        ram_entries = 0
        ram_bytes = 0
        disk_entries = 0
        disk_bytes = 0

        if clear_ram:
            with self._lock:
                ram_entries = len(self._entries)
                ram_bytes = self._total_bytes
                self._entries.clear()
                self._index.clear()
                self._total_bytes = 0

        if clear_disk:
            paths_to_unlink: list[Path] = []
            disk_dir: Path | None = None
            with self._disk_lock:
                disk_entries = len(self._l2_entries)
                disk_bytes = self._l2_total_bytes
                paths_to_unlink = [path for path, _size in self._l2_entries.values()]
                disk_dir = self._disk_dir
                self._l2_entries.clear()
                self._l2_ns_index.clear()
                self._l2_total_bytes = 0
                self._l2_last_touch.clear()

            if paths_to_unlink:
                self._unlink_paths(paths_to_unlink)
            if disk_dir is not None and disk_dir.exists():
                for root, dirs, files in os.walk(disk_dir, followlinks=False):
                    root_path = Path(root)
                    dirs[:] = [d for d in dirs if not (root_path / d).is_symlink()]
                    for fname in files:
                        if fname.endswith(".blk"):
                            fpath = root_path / fname
                            try:
                                if not fpath.is_symlink():
                                    fpath.unlink(missing_ok=True)
                            except OSError:
                                pass

        return {
            "ram_entries_cleared": ram_entries,
            "ram_bytes_cleared": ram_bytes,
            "disk_entries_cleared": disk_entries,
            "disk_bytes_cleared": disk_bytes,
        }

    def clear(self) -> None:
        self.purge(clear_ram=True, clear_disk=True)

    def get_disk_footprint(self) -> tuple[int, int]:
        with self._disk_lock:
            return len(self._l2_entries), self._l2_total_bytes

    def stats(self) -> dict[str, Any]:
        with self._lock:
            l1_enabled = self._max_bytes > 0
            l1_used = self._total_bytes
            l1_max = self._max_bytes
            l1_entries = len(self._entries)
            l1_hits = self._l1_hits
            l1_misses = self._l1_misses
            l1_evictions = self._l1_evictions
            total_hits = self._total_hits
            total_misses = self._total_misses
            block_size = self._block_size

        with self._disk_lock:
            l2_enabled = self.l2_enabled
            l2_dir = str(self._disk_dir) if self._disk_dir is not None else None
            l2_used = self._l2_total_bytes
            l2_max = self._disk_max_bytes
            l2_entries = len(self._l2_entries)
            l2_hits = self._l2_hits
            l2_misses = self._l2_misses
            l2_evictions = self._l2_evictions

        l1_lookups = l1_hits + l1_misses
        l2_lookups = l2_hits + l2_misses
        total_lookups = total_hits + total_misses

        return {
            "enabled": l1_enabled or l2_enabled,
            "used_bytes": l1_used,
            "max_bytes": l1_max,
            "utilization_pct": round((l1_used / l1_max) * 100.0, 2) if l1_max > 0 else 0.0,
            "entries": l1_entries,
            "block_size": block_size,
            "hits": total_hits,
            "misses": total_misses,
            "evictions": l1_evictions,
            "hit_rate_pct": round((total_hits / total_lookups) * 100.0, 2) if total_lookups > 0 else 0.0,
            "l1": {
                "enabled": l1_enabled,
                "used_bytes": l1_used,
                "max_bytes": l1_max,
                "utilization_pct": round((l1_used / l1_max) * 100.0, 2) if l1_max > 0 else 0.0,
                "entries": l1_entries,
                "hits": l1_hits,
                "misses": l1_misses,
                "evictions": l1_evictions,
                "hit_rate_pct": round((l1_hits / l1_lookups) * 100.0, 2) if l1_lookups > 0 else 0.0,
            },
            "l2": {
                "dir": l2_dir,
                "enabled": l2_enabled,
                "used_bytes": l2_used,
                "max_bytes": l2_max,
                "utilization_pct": round((l2_used / l2_max) * 100.0, 2) if l2_max > 0 else 0.0,
                "entries": l2_entries,
                "hits": l2_hits,
                "misses": l2_misses,
                "evictions": l2_evictions,
                "hit_rate_pct": round((l2_hits / l2_lookups) * 100.0, 2) if l2_lookups > 0 else 0.0,
            },
        }


TelegramBlockCache = TwoTierBlockCache


@dataclass
class _InflightEntry:
    task: asyncio.Task[bytes]
    waiters: int = 0
    is_prefetch: bool = False
    skipped: bool = False
    cancelled: bool = False
    error: BaseException | None = None


class StreamFetchCoordinator:
    """Single-flight block coalescing + bounded sequential read-ahead prefetch."""

    def __init__(
        self,
        block_cache: TwoTierBlockCache,
        *,
        prefetch_blocks: int = 2,
        max_prefetch_concurrency: int = 4,
    ) -> None:
        self._block_cache = block_cache
        self._prefetch_blocks = max(0, int(prefetch_blocks))
        self._max_prefetch_concurrency = max(0, int(max_prefetch_concurrency))

        self._sem_loop: asyncio.AbstractEventLoop | None = None
        self._prefetch_sem: asyncio.Semaphore | None = None
        self._shutting_down = False

        self._inflight: dict[tuple[Any, int], _InflightEntry] = {}
        self._prefetch_tasks: dict[
            asyncio.Task[Any], tuple[Any, tuple[Any, int], _InflightEntry]
        ] = {}
        self._warm_tasks: dict[asyncio.Task[Any], tuple[Any, Any]] = {}
        self._warm_keys: set[Any] = set()

        self._prefetched_keys: OrderedDict[tuple[Any, int], None] = OrderedDict()
        self._seq_state: OrderedDict[Any, tuple[int, int]] = OrderedDict()
        self._eof_offsets: OrderedDict[Any, int] = OrderedDict()

        # Telemetry
        self._coalesced_waits = 0
        self._prefetch_scheduled = 0
        self._prefetch_completed = 0
        self._prefetch_hits = 0
        self._prefetch_drops = 0
        self._prefetch_errors = 0

    def configure(
        self,
        *,
        prefetch_blocks: int | None = None,
        max_prefetch_concurrency: int | None = None,
    ) -> None:
        if prefetch_blocks is not None:
            self._prefetch_blocks = max(0, int(prefetch_blocks))
        if max_prefetch_concurrency is not None:
            new_cap = max(0, int(max_prefetch_concurrency))
            if new_cap != self._max_prefetch_concurrency:
                self._max_prefetch_concurrency = new_cap
                try:
                    running_loop = asyncio.get_running_loop()
                except RuntimeError:
                    running_loop = None
                if running_loop is not None and self._sem_loop is running_loop:
                    self._prefetch_sem = asyncio.Semaphore(max(1, new_cap))
                else:
                    self._prefetch_sem = None

    def _prune_stale_loop_tasks(self, current_loop: asyncio.AbstractEventLoop) -> None:
        for key, entry in list(self._inflight.items()):
            t = entry.task
            if t.get_loop() is not current_loop or t.done():
                self._inflight.pop(key, None)
        for task in list(self._prefetch_tasks):
            if task.get_loop() is not current_loop or task.done():
                self._prefetch_tasks.pop(task, None)
        for task, (wkey, _ns) in list(self._warm_tasks.items()):
            if task.get_loop() is not current_loop or task.done():
                self._warm_tasks.pop(task, None)
                self._warm_keys.discard(wkey)

    def _ensure_loop_state(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self._sem_loop is not loop:
            self._sem_loop = loop
            self._prefetch_sem = asyncio.Semaphore(max(1, self._max_prefetch_concurrency))
            self._shutting_down = False
            self._prune_stale_loop_tasks(loop)
        elif self._prefetch_sem is None:
            self._prefetch_sem = asyncio.Semaphore(max(1, self._max_prefetch_concurrency))
        return loop

    def _active_bg_count(self) -> int:
        for task in list(self._prefetch_tasks):
            if task.done():
                self._prefetch_tasks.pop(task, None)
        for task, (wkey, _ns) in list(self._warm_tasks.items()):
            if task.done():
                self._warm_tasks.pop(task, None)
                self._warm_keys.discard(wkey)
        return len(self._prefetch_tasks) + len(self._warm_tasks)

    def _record_prefetched_key(self, cache_ns: Any, off: int) -> None:
        key = (cache_ns, off)
        self._prefetched_keys[key] = None
        self._prefetched_keys.move_to_end(key)
        while len(self._prefetched_keys) > _MAX_PREFETCHED_KEYS:
            self._prefetched_keys.popitem(last=False)

    def note_cache_hit(self, cache_ns: Any, block_offset: int) -> bool:
        key = (cache_ns, int(block_offset))
        if key in self._prefetched_keys:
            self._prefetched_keys.pop(key, None)
            self._prefetch_hits += 1
            return True
        return False

    def record_eof(self, cache_ns: Any, eof_offset: int) -> None:
        self._eof_offsets[cache_ns] = max(0, int(eof_offset))
        self._eof_offsets.move_to_end(cache_ns)
        while len(self._eof_offsets) > _MAX_SEQ_NAMESPACES:
            self._eof_offsets.popitem(last=False)

    def clear_eof(self, cache_ns: Any) -> None:
        self._eof_offsets.pop(cache_ns, None)

    def should_prefetch(self, cache_ns: Any, req_start: int, req_end: int, block_size: int) -> bool:
        if req_start > req_end or block_size <= 0:
            return False

        prev = self._seq_state.get(cache_ns)
        self._seq_state[cache_ns] = (req_start, req_end)
        self._seq_state.move_to_end(cache_ns)
        while len(self._seq_state) > _MAX_SEQ_NAMESPACES:
            self._seq_state.popitem(last=False)

        multi_block = (req_end // block_size) > (req_start // block_size)
        entering_new_block = False

        if prev is not None:
            prev_start, prev_end = prev
            is_contiguous = (prev_end + 1) <= req_start <= (prev_end + _SEQ_GAP_TOLERANCE_BYTES)
            is_same_or_overlap = prev_start <= req_start <= prev_end
            if not is_contiguous and not is_same_or_overlap:
                self.cancel_prefetch_for_namespace(cache_ns)
            elif is_contiguous:
                prev_end_block = prev_end // block_size
                if (req_start // block_size) > prev_end_block or (req_end // block_size) > prev_end_block:
                    entering_new_block = True

        if self._prefetch_blocks <= 0 or not self._block_cache.enabled:
            return False
        return bool(multi_block or entering_new_block)

    async def fetch_block(
        self,
        cache_ns: Any,
        block_offset: int,
        fetch_fn: Callable[[], Coroutine[Any, Any, bytes]],
    ) -> tuple[bytes, str]:
        loop = self._ensure_loop_state()
        off = int(block_offset)
        key = (cache_ns, off)

        while True:
            entry = self._inflight.get(key)
            if (
                entry is not None
                and not entry.cancelled
                and entry.task.get_loop() is loop
                and not entry.task.done()
                and entry.task.cancelling() == 0
            ):
                entry.waiters += 1
                self._coalesced_waits += 1
                if entry.is_prefetch:
                    self._prefetch_hits += 1
                    source = "prefetch_hit"
                else:
                    source = "coalesced"
            else:
                source = "fetch"
                new_entry: _InflightEntry

                async def _run_foreground() -> bytes:
                    try:
                        data = await fetch_fn()
                        if data and self._block_cache.enabled:
                            await self._block_cache.put_block_async(cache_ns, off, data)
                        if len(data) < self._block_cache.block_size:
                            self.record_eof(cache_ns, off + len(data))
                        return data
                    finally:
                        if self._inflight.get(key) is new_entry:
                            self._inflight.pop(key, None)

                task = loop.create_task(_run_foreground(), name=f"stream-fetch:{off}")
                new_entry = _InflightEntry(task=task, waiters=1, is_prefetch=False)
                self._inflight[key] = new_entry
                entry = new_entry

            try:
                try:
                    data = await asyncio.shield(entry.task)
                except asyncio.CancelledError:
                    cur_task = asyncio.current_task()
                    if cur_task is not None and cur_task.cancelling() > 0:
                        raise
                    if self._inflight.get(key) is entry:
                        self._inflight.pop(key, None)
                    continue
                if entry.error is not None:
                    raise entry.error
                if entry.skipped:
                    if self._inflight.get(key) is entry:
                        self._inflight.pop(key, None)
                    cached_data, _ = await self._block_cache.get_block_async(cache_ns, off)
                    if cached_data is not None:
                        return cached_data, "coalesced"
                    continue
                return data, source
            finally:
                entry.waiters = max(0, entry.waiters - 1)
                if entry.waiters == 0 and not entry.is_prefetch and not entry.task.done():
                    entry.cancelled = True
                    if self._inflight.get(key) is entry:
                        self._inflight.pop(key, None)
                    entry.task.cancel()
                    try:
                        await entry.task
                    except (asyncio.CancelledError, Exception):
                        pass

    def schedule_prefetch(
        self,
        cache_ns: Any,
        block_offset: int,
        fetch_fn: Callable[[], Coroutine[Any, Any, bytes]],
        *,
        can_run_fn: Callable[[], bool] | None = None,
        on_error: Callable[[Exception], Any] | None = None,
    ) -> bool:
        loop = self._ensure_loop_state()
        sem = self._prefetch_sem
        assert sem is not None
        off = int(block_offset)
        if off < 0:
            return False
        if self._shutting_down or self._prefetch_blocks <= 0 or not self._block_cache.enabled:
            return False

        eof = self._eof_offsets.get(cache_ns)
        if eof is not None and off >= eof:
            return False
        if self._block_cache.has_block(cache_ns, off):
            return False

        key = (cache_ns, off)
        existing = self._inflight.get(key)
        if (
            existing is not None
            and not existing.cancelled
            and existing.task.get_loop() is loop
            and not existing.task.done()
            and existing.task.cancelling() == 0
        ):
            return False

        if self._active_bg_count() >= self._max_prefetch_concurrency:
            self._prefetch_drops += 1
            return False
        if can_run_fn is not None and not can_run_fn():
            self._prefetch_drops += 1
            return False

        entry_holder: list[_InflightEntry] = []
        task_holder: list[asyncio.Task[bytes]] = []

        async def _run_prefetch() -> bytes:
            entry = entry_holder[0]
            task = task_holder[0]
            try:
                async with sem:
                    if self._shutting_down:
                        entry.skipped = True
                        return b""
                    if entry.waiters == 0 and can_run_fn is not None and not can_run_fn():
                        entry.skipped = True
                        self._prefetch_drops += 1
                        return b""
                    if self._block_cache.has_block(cache_ns, off):
                        entry.skipped = True
                        return b""
                    data = await fetch_fn()
                    if data and self._block_cache.enabled:
                        await self._block_cache.put_block_async(cache_ns, off, data)
                        if entry.waiters == 0:
                            self._record_prefetched_key(cache_ns, off)
                    if len(data) < self._block_cache.block_size:
                        self.record_eof(cache_ns, off + len(data))
                    if data:
                        self._prefetch_completed += 1
                    return data
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                entry.error = exc
                self._prefetch_errors += 1
                if on_error is not None:
                    try:
                        on_error(exc)
                    except Exception:
                        pass
                return b""
            finally:
                self._prefetch_tasks.pop(task, None)
                if self._inflight.get(key) is entry:
                    self._inflight.pop(key, None)

        task = loop.create_task(_run_prefetch(), name=f"stream-prefetch:{off}")
        entry = _InflightEntry(task=task, waiters=0, is_prefetch=True)
        entry_holder.append(entry)
        task_holder.append(task)
        self._inflight[key] = entry
        self._prefetch_tasks[task] = (cache_ns, key, entry)
        self._prefetch_scheduled += 1
        return True

    def schedule_background_warm(
        self,
        key: Any,
        coro_factory: Callable[[], Coroutine[Any, Any, Any]],
        *,
        namespace: Any,
        on_error: Callable[[Exception], Any] | None = None,
    ) -> bool:
        loop = self._ensure_loop_state()
        sem = self._prefetch_sem
        assert sem is not None
        if self._shutting_down:
            return False
        if key in self._warm_keys:
            return False
        if self._active_bg_count() >= self._max_prefetch_concurrency:
            self._prefetch_drops += 1
            return False

        self._warm_keys.add(key)
        self._prefetch_scheduled += 1
        task_holder: list[asyncio.Task[None]] = []

        async def _run_warm() -> None:
            task = task_holder[0]
            try:
                async with sem:
                    if self._shutting_down:
                        return
                    await coro_factory()
                    self._prefetch_completed += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._prefetch_errors += 1
                if on_error is not None:
                    try:
                        on_error(exc)
                    except Exception:
                        pass
            finally:
                self._warm_keys.discard(key)
                self._warm_tasks.pop(task, None)

        task = loop.create_task(_run_warm(), name=f"stream-warm:{key}")
        task_holder.append(task)
        self._warm_tasks[task] = (key, namespace)
        return True

    def cancel_prefetch_for_namespace(self, cache_ns: Any) -> int:
        cancelled = 0
        for task, (ns, key, entry) in list(self._prefetch_tasks.items()):
            if ns == cache_ns and entry.waiters == 0 and not task.done():
                entry.cancelled = True
                if self._inflight.get(key) is entry:
                    self._inflight.pop(key, None)
                task.cancel()
                cancelled += 1
        for task, (_wkey, ns) in list(self._warm_tasks.items()):
            if ns == cache_ns and not task.done():
                task.cancel()
                cancelled += 1
        return cancelled

    def invalidate_namespace(self, cache_ns: Any) -> int:
        cancelled = self.cancel_prefetch_for_namespace(cache_ns)
        self._eof_offsets.pop(cache_ns, None)
        self._seq_state.pop(cache_ns, None)
        for key in [k for k in self._prefetched_keys if k[0] == cache_ns]:
            self._prefetched_keys.pop(key, None)
        return cancelled

    async def cancel_all_background(self) -> int:
        """Cancel and await idle prefetch & warm tasks without shutting down the coordinator."""
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        targets: list[asyncio.Task[Any]] = []
        for task, (_ns, key, entry) in list(self._prefetch_tasks.items()):
            if entry.waiters == 0:
                entry.cancelled = True
                if self._inflight.get(key) is entry:
                    self._inflight.pop(key, None)
                if not task.done():
                    task.cancel()
                if current_loop is None or task.get_loop() is current_loop:
                    targets.append(task)
        for task in list(self._warm_tasks):
            if not task.done():
                task.cancel()
            if current_loop is None or task.get_loop() is current_loop:
                targets.append(task)

        if targets:
            await asyncio.gather(*targets, return_exceptions=True)

        self._prefetched_keys.clear()
        self._eof_offsets.clear()
        self._seq_state.clear()
        return len(targets)

    async def stop(self) -> None:
        self._shutting_down = True
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        candidates: list[asyncio.Task[Any]] = []
        for task, (_ns, key, entry) in list(self._prefetch_tasks.items()):
            entry.cancelled = True
            if self._inflight.get(key) is entry:
                self._inflight.pop(key, None)
            if not task.done():
                task.cancel()
            candidates.append(task)
        for task in list(self._warm_tasks):
            if not task.done():
                task.cancel()
            candidates.append(task)
        for key, entry in list(self._inflight.items()):
            entry.cancelled = True
            self._inflight.pop(key, None)
            if not entry.task.done():
                entry.task.cancel()
            candidates.append(entry.task)

        unique_tasks: list[asyncio.Task[Any]] = []
        seen: set[int] = set()
        for task in candidates:
            tid = id(task)
            if tid not in seen:
                seen.add(tid)
                if current_loop is None or task.get_loop() is current_loop:
                    unique_tasks.append(task)

        if unique_tasks:
            await asyncio.gather(*unique_tasks, return_exceptions=True)

        self._prefetch_tasks.clear()
        self._warm_tasks.clear()
        self._warm_keys.clear()
        self._inflight.clear()

    def stats(self) -> dict[str, Any]:
        return {
            "coalesced_waits": self._coalesced_waits,
            "prefetch": {
                "enabled": self._prefetch_blocks > 0 and self._block_cache.enabled,
                "blocks_ahead": self._prefetch_blocks,
                "max_concurrency": self._max_prefetch_concurrency,
                "active_tasks": self._active_bg_count(),
                "scheduled": self._prefetch_scheduled,
                "completed": self._prefetch_completed,
                "hits": self._prefetch_hits,
                "drops": self._prefetch_drops,
                "errors": self._prefetch_errors,
            },
        }
