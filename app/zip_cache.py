from __future__ import annotations

import hashlib
import json
import threading
import zlib
from collections import OrderedDict
from collections.abc import Iterator, MutableMapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from app.db import Database

_MISSING = object()


def compute_zip_fingerprint(item: dict[str, Any]) -> str:
    """Compute a deterministic 32-char SHA-256 hex fingerprint for a media item's ZIP location/content."""
    raw_parts = item.get("parts") or []
    norm_parts: list[tuple[int, str, int, int]] = []
    for p in raw_parts:
        if not isinstance(p, dict):
            continue
        p_idx = int(p.get("part_index") or 0)
        p_chat = str(p.get("telegram_chat_id") or "")
        p_msg = int(p.get("telegram_msg_id") or 0)
        p_size = int(p.get("size_bytes") or 0)
        norm_parts.append((p_idx, p_chat, p_msg, p_size))
    norm_parts.sort()

    msg_raw = item.get("telegram_msg_id")
    msg_id = int(msg_raw) if msg_raw is not None and msg_raw != "" else None
    pc_raw = item.get("part_count")
    part_count = int(pc_raw) if pc_raw is not None else 1

    payload = {
        "size_bytes": int(item.get("size_bytes") or 0),
        "sha256": str(item.get("sha256") or "").strip().lower(),
        "file_path": str(item.get("file_path") or ""),
        "telegram_chat_id": str(item.get("telegram_chat_id") or ""),
        "telegram_msg_id": msg_id,
        "part_count": part_count,
        "parts": norm_parts,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:32]


class ZipDirectoryCache(MutableMapping[str, list[dict[str, Any]]]):
    """Two-tier (bounded in-memory LRU + SQLite-persisted) ZIP central directory cache."""

    def __init__(
        self,
        db: Database | None = None,
        *,
        max_l1_items: int = 256,
        max_db_rows: int = 500,
        max_db_bytes: int = 16 * 1024 * 1024,
    ) -> None:
        self._db = db
        self._max_l1_items = max(0, int(max_l1_items))
        self._max_db_rows = max(0, int(max_db_rows))
        self._max_db_bytes = max(0, int(max_db_bytes))
        self._lock = threading.RLock()
        self._l1: OrderedDict[str, tuple[str, list[dict[str, Any]]]] = OrderedDict()

        self._l1_hits = 0
        self._l2_hits = 0
        self._misses = 0
        self._l1_evictions = 0
        self._l2_evictions = 0
        self._invalidations = 0

    def _clone_entries(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [dict(e) if isinstance(e, dict) else e for e in entries]

    def _put_l1(self, media_id: str, fingerprint: str, entries: list[dict[str, Any]]) -> None:
        if self._max_l1_items <= 0:
            return
        if media_id in self._l1:
            self._l1[media_id] = (fingerprint, entries)
            self._l1.move_to_end(media_id)
            return
        while len(self._l1) >= self._max_l1_items:
            self._l1.popitem(last=False)
            self._l1_evictions += 1
        self._l1[media_id] = (fingerprint, entries)

    def get_entries(
        self, media_id: str, *, fingerprint: str | None = None
    ) -> list[dict[str, Any]] | None:
        with self._lock:
            invalidated = False
            l1_item = self._l1.get(media_id)
            if l1_item is not None:
                cached_fp, entries = l1_item
                if fingerprint is None or cached_fp == fingerprint:
                    self._l1.move_to_end(media_id)
                    self._l1_hits += 1
                    return self._clone_entries(entries)
                del self._l1[media_id]
                invalidated = True

            if self._db is not None:
                row = self._db.get_zip_cache_entry(media_id)
                if row is not None:
                    row_fp = row["fingerprint"]
                    if fingerprint is not None and row_fp != fingerprint:
                        self._db.delete_zip_cache_entry(media_id)
                        self._invalidations += 1
                        self._misses += 1
                        return None
                    try:
                        raw = zlib.decompress(row["entries_blob"])
                        parsed = json.loads(raw.decode("utf-8"))
                        if not isinstance(parsed, list):
                            raise ValueError("ZIP cache payload must be a JSON list")
                    except Exception:
                        self._db.delete_zip_cache_entry(media_id)
                        self._invalidations += 1
                        self._misses += 1
                        return None

                    cloned = self._clone_entries(parsed)
                    self._put_l1(media_id, row_fp, cloned)
                    self._l2_hits += 1
                    return self._clone_entries(cloned)

            if invalidated:
                self._invalidations += 1
            self._misses += 1
            return None

    def set_entries(
        self,
        media_id: str,
        entries: list[dict[str, Any]],
        *,
        fingerprint: str = "",
    ) -> None:
        with self._lock:
            stored = self._clone_entries(entries)
            self._put_l1(media_id, fingerprint, stored)
            if self._db is not None:
                payload = json.dumps(stored, separators=(",", ":")).encode("utf-8")
                compressed = zlib.compress(payload, level=6)
                evicted = self._db.put_zip_cache_entry(
                    media_id,
                    fingerprint=fingerprint,
                    entry_count=len(stored),
                    entries_blob=compressed,
                    max_rows=self._max_db_rows,
                    max_bytes=self._max_db_bytes,
                )
                self._l2_evictions += evicted

    def invalidate(self, media_id: str) -> bool:
        with self._lock:
            removed_l1 = self._l1.pop(media_id, None) is not None
            removed_l2 = (
                self._db.delete_zip_cache_entry(media_id)
                if self._db is not None
                else False
            )
            if removed_l1 or removed_l2:
                self._invalidations += 1
                return True
            return False

    def purge(self) -> dict[str, int]:
        with self._lock:
            l1_cleared = len(self._l1)
            self._l1.clear()
            l2_cleared = self._db.clear_zip_cache() if self._db is not None else 0
            return {"l1_cleared": l1_cleared, "l2_cleared": l2_cleared}

    def stats(self) -> dict[str, Any]:
        with self._lock:
            db_stats = (
                self._db.get_zip_cache_db_stats()
                if self._db is not None
                else {"entries": 0, "total_bytes": 0}
            )
            hits = self._l1_hits + self._l2_hits
            total = hits + self._misses
            hit_rate_pct = round((hits / total) * 100.0, 2) if total > 0 else 0.0
            return {
                "l1_entries": len(self._l1),
                "l1_max_entries": self._max_l1_items,
                "l2_entries": int(db_stats["entries"]),
                "l2_max_entries": self._max_db_rows,
                "l2_bytes": int(db_stats["total_bytes"]),
                "l2_max_bytes": self._max_db_bytes,
                "l1_hits": self._l1_hits,
                "l2_hits": self._l2_hits,
                "hits": hits,
                "misses": self._misses,
                "hit_rate_pct": hit_rate_pct,
                "l1_evictions": self._l1_evictions,
                "l2_evictions": self._l2_evictions,
                "invalidations": self._invalidations,
            }

    # MutableMapping backwards-compatibility interface
    def __contains__(self, media_id: object) -> bool:
        if not isinstance(media_id, str):
            return False
        with self._lock:
            if media_id in self._l1:
                return True
            if self._db is not None:
                return self._db.has_zip_cache_entry(media_id)
            return False

    def __getitem__(self, media_id: str) -> list[dict[str, Any]]:
        entries = self.get_entries(media_id)
        if entries is None:
            raise KeyError(media_id)
        return entries

    def __setitem__(self, media_id: str, entries: list[dict[str, Any]]) -> None:
        self.set_entries(media_id, entries)

    def __delitem__(self, media_id: str) -> None:
        if not self.invalidate(media_id):
            raise KeyError(media_id)

    def get(self, media_id: str, default: Any = None) -> Any:
        entries = self.get_entries(media_id)
        return entries if entries is not None else default

    def pop(self, media_id: str, default: Any = _MISSING) -> Any:
        with self._lock:
            l1_item = self._l1.pop(media_id, None)
            entries: list[dict[str, Any]] | None = None
            if l1_item is not None:
                entries = self._clone_entries(l1_item[1])
            removed_l2 = False
            if self._db is not None:
                if entries is None:
                    row = self._db.get_zip_cache_entry(media_id)
                    if row is not None:
                        try:
                            raw = zlib.decompress(row["entries_blob"])
                            parsed = json.loads(raw.decode("utf-8"))
                            if isinstance(parsed, list):
                                entries = self._clone_entries(parsed)
                        except Exception:
                            entries = None
                removed_l2 = self._db.delete_zip_cache_entry(media_id)
            if l1_item is not None or removed_l2:
                self._invalidations += 1
            if entries is None:
                if default is _MISSING:
                    raise KeyError(media_id)
                return default
            return entries

    def clear(self) -> None:
        self.purge()

    def _all_keys(self) -> list[str]:
        keys = list(self._l1.keys())
        if self._db is not None:
            seen = set(keys)
            for db_id in self._db.list_zip_cache_media_ids():
                if db_id not in seen:
                    seen.add(db_id)
                    keys.append(db_id)
        return keys

    def __len__(self) -> int:
        with self._lock:
            return len(self._all_keys())

    def __iter__(self) -> Iterator[str]:
        with self._lock:
            keys = self._all_keys()
        return iter(keys)
