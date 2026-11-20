from __future__ import annotations

import json
import threading
import time
import zlib
from pathlib import Path

import pytest

from app.db import Database
from app.zip_cache import ZipDirectoryCache, compute_zip_fingerprint


def _sample_entries(prefix: str = "file", count: int = 2) -> list[dict]:
    return [
        {
            "path": f"dir/{prefix}_{i}.txt",
            "name": f"{prefix}_{i}.txt",
            "size_bytes": 100 + i,
            "compress_size": 80 + i,
            "compress_type": 8,
            "header_offset": i * 128,
            "media_type": "file",
            "mime_type": "text/plain",
        }
        for i in range(count)
    ]


def test_compute_zip_fingerprint_determinism_and_sensitivity():
    base_item = {
        "id": "m1",
        "title": "Archive One",
        "folder": "General",
        "tags": "zip,docs",
        "size_bytes": 2048,
        "sha256": "ABCD1234",
        "file_path": "/data/archive.zip",
        "telegram_chat_id": "-10012345",
        "telegram_msg_id": 42,
        "part_count": 2,
        "parts": [
            {
                "part_index": 1,
                "telegram_chat_id": "-10012345",
                "telegram_msg_id": 43,
                "size_bytes": 1024,
            },
            {
                "part_index": 0,
                "telegram_chat_id": "-10012345",
                "telegram_msg_id": 42,
                "size_bytes": 1024,
            },
        ],
    }

    fp1 = compute_zip_fingerprint(base_item)
    assert len(fp1) == 32
    assert fp1 == compute_zip_fingerprint(dict(base_item))

    # Reordered parts and metadata-only edits must keep identical fingerprint
    reordered = {
        **base_item,
        "title": "Completely Different Title",
        "folder": "Other",
        "tags": "changed",
        "parts": list(reversed(base_item["parts"])),
    }
    assert compute_zip_fingerprint(reordered) == fp1

    # Structural/content changes must alter the fingerprint
    assert compute_zip_fingerprint({**base_item, "size_bytes": 2049}) != fp1
    assert compute_zip_fingerprint({**base_item, "sha256": "FFFF9999"}) != fp1
    assert compute_zip_fingerprint({**base_item, "file_path": "/data/moved.zip"}) != fp1
    assert compute_zip_fingerprint({**base_item, "telegram_chat_id": "-10099999"}) != fp1
    assert compute_zip_fingerprint({**base_item, "telegram_msg_id": 99}) != fp1
    assert compute_zip_fingerprint({**base_item, "part_count": 3}) != fp1
    changed_part = {
        **base_item,
        "parts": [
            base_item["parts"][0],
            {**base_item["parts"][1], "telegram_msg_id": 420},
        ],
    }
    assert compute_zip_fingerprint(changed_part) != fp1


def test_l1_hit_miss_and_lru_eviction(tmp_path: Path):
    # Pure L1 cache (no DB)
    mem_cache = ZipDirectoryCache(db=None, max_l1_items=2)
    assert mem_cache.get_entries("missing") is None
    assert mem_cache.stats()["misses"] == 1

    e1 = _sample_entries("a")
    e2 = _sample_entries("b")
    e3 = _sample_entries("c")

    mem_cache.set_entries("m1", e1, fingerprint="fp1")
    mem_cache.set_entries("m2", e2, fingerprint="fp2")
    # Access m1 so m2 becomes least recently used
    assert mem_cache.get_entries("m1", fingerprint="fp1") == e1
    # Adding m3 evicts m2
    mem_cache.set_entries("m3", e3, fingerprint="fp3")

    assert mem_cache.get_entries("m2", fingerprint="fp2") is None
    assert mem_cache.get_entries("m1", fingerprint="fp1") == e1
    assert mem_cache.get_entries("m3", fingerprint="fp3") == e3

    st = mem_cache.stats()
    assert st["l1_entries"] == 2
    assert st["l1_evictions"] == 1
    assert st["l1_hits"] == 3
    assert st["l2_hits"] == 0
    assert st["misses"] == 2

    # With L2 DB, evicted L1 entry falls back to L2 and re-promotes
    db = Database(tmp_path / "l1_lru.db")
    try:
        two_tier = ZipDirectoryCache(db=db, max_l1_items=2, max_db_rows=10)
        two_tier.set_entries("m1", e1, fingerprint="fp1")
        two_tier.set_entries("m2", e2, fingerprint="fp2")
        assert two_tier.get_entries("m1", fingerprint="fp1") == e1
        two_tier.set_entries("m3", e3, fingerprint="fp3")

        # m2 was evicted from L1, so fetching it hits L2 and promotes back into L1 (evicting m1)
        assert two_tier.get_entries("m2", fingerprint="fp2") == e2
        st2 = two_tier.stats()
        assert st2["l2_hits"] == 1
        assert st2["l1_evictions"] == 2
        # Subsequent m2 lookup is now an L1 hit
        assert two_tier.get_entries("m2", fingerprint="fp2") == e2
        assert two_tier.stats()["l1_hits"] == 2
    finally:
        db.close()


def test_persistence_across_restart_promotes_l2_to_l1(tmp_path: Path):
    db_path = tmp_path / "restart.db"
    entries = _sample_entries("persist", count=5)
    fp = "fp_persist_1234"

    db1 = Database(db_path)
    cache1 = ZipDirectoryCache(db=db1, max_l1_items=16)
    cache1.set_entries("zip_1", entries, fingerprint=fp)
    assert cache1.stats()["l1_entries"] == 1
    assert cache1.stats()["l2_entries"] == 1
    db1.close()

    # Simulated process restart
    db2 = Database(db_path)
    try:
        cache2 = ZipDirectoryCache(db=db2, max_l1_items=16)
        st_before = cache2.stats()
        assert st_before["l1_entries"] == 0
        assert st_before["l2_entries"] == 1
        assert st_before["l2_bytes"] > 0

        loaded = cache2.get_entries("zip_1", fingerprint=fp)
        assert loaded == entries

        st_after = cache2.stats()
        assert st_after["l2_hits"] == 1
        assert st_after["l1_hits"] == 0
        assert st_after["l1_entries"] == 1

        # Second read hits promoted L1
        assert cache2.get_entries("zip_1", fingerprint=fp) == entries
        assert cache2.stats()["l1_hits"] == 1
        assert cache2.stats()["l2_hits"] == 1
    finally:
        db2.close()


def test_fingerprint_mismatch_auto_invalidates_l1_and_l2(tmp_path: Path):
    db_path = tmp_path / "fp_mismatch.db"
    db = Database(db_path)
    try:
        cache = ZipDirectoryCache(db=db, max_l1_items=10)
        entries = _sample_entries("stale")

        # L1 + L2 mismatch
        cache.set_entries("m1", entries, fingerprint="fp_old")
        assert cache.get_entries("m1", fingerprint="fp_new") is None
        assert db.has_zip_cache_entry("m1") is False
        assert cache.stats()["invalidations"] >= 1
        assert cache.stats()["misses"] == 1

        # L2-only mismatch after restart
        cache.set_entries("m2", entries, fingerprint="fp_v1")
    finally:
        db.close()

    db2 = Database(db_path)
    try:
        cache2 = ZipDirectoryCache(db=db2, max_l1_items=10)
        assert db2.has_zip_cache_entry("m2") is True
        assert cache2.get_entries("m2", fingerprint="fp_v2") is None
        assert db2.has_zip_cache_entry("m2") is False
        assert cache2.stats()["invalidations"] == 1
        assert cache2.stats()["misses"] == 1
    finally:
        db2.close()


def test_db_delete_and_structural_update_invalidation_vs_metadata_update(tmp_path: Path):
    db = Database(tmp_path / "media_sync.db")
    try:
        item = db.insert_media(
            media_id="zip_media_1",
            title="Bundle",
            filename="bundle.zip",
            folder="Archives",
            media_type="file",
            mime_type="application/zip",
            size_bytes=4096,
            drive_name="local",
            file_path="/vault/bundle.zip",
            thumb_filename=None,
            source_origin="web",
            source_kind="upload",
            source_url=None,
            tags="archive",
            telegram_chat_id="-1001",
            telegram_msg_id=10,
            part_count=1,
        )
        fp = compute_zip_fingerprint(item)
        cache = ZipDirectoryCache(db=db)
        entries = _sample_entries("bundle", count=3)
        cache.set_entries("zip_media_1", entries, fingerprint=fp)
        assert db.get_db_stats()["zip_cache_entries"] == 1

        # Metadata-only update preserves valid cache
        updated_meta = db.update_media(
            "zip_media_1",
            title="Bundle Renamed",
            folder="Docs",
            tags="archive,important",
            telegram_topic_id=77,
            telegram_worker_name="worker_b",
        )
        assert updated_meta is not None
        assert db.has_zip_cache_entry("zip_media_1") is True
        assert compute_zip_fingerprint(updated_meta) == fp
        assert cache.get_entries("zip_media_1", fingerprint=fp) == entries

        # Structural updates invalidate zip_entries_cache
        db.update_media("zip_media_1", file_path="/vault/bundle_v2.zip")
        assert db.has_zip_cache_entry("zip_media_1") is False

        cache.set_entries("zip_media_1", entries, fingerprint="fp_a")
        db.update_media("zip_media_1", telegram_msg_id=11)
        assert db.has_zip_cache_entry("zip_media_1") is False

        cache.set_entries("zip_media_1", entries, fingerprint="fp_b")
        db.update_media("zip_media_1", telegram_chat_id="-1002")
        assert db.has_zip_cache_entry("zip_media_1") is False

        cache.set_entries("zip_media_1", entries, fingerprint="fp_c")
        db.update_media("zip_media_1", part_count=2)
        assert db.has_zip_cache_entry("zip_media_1") is False

        cache.set_entries("zip_media_1", entries, fingerprint="fp_d")
        db.update_media(
            "zip_media_1",
            parts=[
                {
                    "part_index": 0,
                    "start_byte": 0,
                    "end_byte": 4095,
                    "size_bytes": 4096,
                    "telegram_chat_id": "-1002",
                    "telegram_msg_id": 12,
                }
            ],
        )
        assert db.has_zip_cache_entry("zip_media_1") is False

        # delete_media removes zip_entries_cache row
        cache.set_entries("zip_media_1", entries, fingerprint="fp_e")
        assert db.has_zip_cache_entry("zip_media_1") is True
        deleted = db.delete_media("zip_media_1")
        assert deleted is not None
        assert db.has_zip_cache_entry("zip_media_1") is False
        assert db.get_db_stats()["zip_cache_entries"] == 0
    finally:
        db.close()


def test_sqlite_lru_bounding_by_max_rows_and_max_bytes(tmp_path: Path):
    # Row bounding
    db_rows = Database(tmp_path / "bounded_rows.db")
    try:
        cache = ZipDirectoryCache(db=db_rows, max_l1_items=0, max_db_rows=2, max_db_bytes=1024 * 1024)
        cache.set_entries("r1", _sample_entries("r1"), fingerprint="f1")
        time.sleep(0.002)
        cache.set_entries("r2", _sample_entries("r2"), fingerprint="f2")
        time.sleep(0.002)
        # Touch r1 so r2 becomes oldest-accessed
        assert cache.get_entries("r1", fingerprint="f1") is not None
        time.sleep(0.002)
        cache.set_entries("r3", _sample_entries("r3"), fingerprint="f3")

        ids = db_rows.list_zip_cache_media_ids()
        assert set(ids) == {"r1", "r3"}
        assert db_rows.has_zip_cache_entry("r2") is False
        assert cache.stats()["l2_evictions"] == 1
    finally:
        db_rows.close()

    # Byte bounding
    db_bytes = Database(tmp_path / "bounded_bytes.db")
    try:
        # Measure compressed size of a single entry payload
        sample = _sample_entries("incompressible", count=15)
        for idx, item in enumerate(sample):
            item["path"] = f"dir/{idx}_{'x' * 24}_{idx * 7919}.bin"
        compressed_size = len(zlib.compress(json.dumps(sample, separators=(",", ":")).encode("utf-8"), level=6))
        max_bytes = compressed_size * 2 + 10  # fits 2 entries, not 3

        cache_b = ZipDirectoryCache(
            db=db_bytes, max_l1_items=0, max_db_rows=100, max_db_bytes=max_bytes
        )
        cache_b.set_entries("b1", sample, fingerprint="fb1")
        time.sleep(0.002)
        cache_b.set_entries("b2", sample, fingerprint="fb2")
        time.sleep(0.002)
        # Touch b1 so b2 is evicted when b3 arrives
        assert cache_b.get_entries("b1", fingerprint="fb1") is not None
        time.sleep(0.002)
        cache_b.set_entries("b3", sample, fingerprint="fb3")

        stats = db_bytes.get_zip_cache_db_stats()
        assert stats["entries"] == 2
        assert stats["total_bytes"] <= max_bytes
        assert set(db_bytes.list_zip_cache_media_ids()) == {"b1", "b3"}

        # Oversized single entry (> max_bytes) is not stored and deletes any stale row
        evicted = db_bytes.put_zip_cache_entry(
            "b1",
            fingerprint="oversized",
            entry_count=1,
            entries_blob=b"x" * (max_bytes + 1),
            max_rows=100,
            max_bytes=max_bytes,
        )
        assert evicted == 0
        assert db_bytes.has_zip_cache_entry("b1") is False

        # Non-positive limits do not store
        assert (
            db_bytes.put_zip_cache_entry(
                "b3",
                fingerprint="zero",
                entry_count=1,
                entries_blob=b"abc",
                max_rows=0,
                max_bytes=max_bytes,
            )
            == 0
        )
        assert db_bytes.has_zip_cache_entry("b3") is False
    finally:
        db_bytes.close()


def test_corrupt_entries_blob_self_heals(tmp_path: Path):
    db = Database(tmp_path / "corrupt.db")
    try:
        cache = ZipDirectoryCache(db=db, max_l1_items=10)

        # Case 1: invalid zlib bytes
        db.put_zip_cache_entry(
            "corrupt_zlib",
            fingerprint="fp1",
            entry_count=1,
            entries_blob=b"not-valid-zlib-data",
        )
        assert db.has_zip_cache_entry("corrupt_zlib") is True
        assert cache.get_entries("corrupt_zlib", fingerprint="fp1") is None
        assert db.has_zip_cache_entry("corrupt_zlib") is False

        # Case 2: valid zlib, invalid JSON
        db.put_zip_cache_entry(
            "corrupt_json",
            fingerprint="fp2",
            entry_count=1,
            entries_blob=zlib.compress(b"{broken json"),
        )
        assert cache.get_entries("corrupt_json", fingerprint="fp2") is None
        assert db.has_zip_cache_entry("corrupt_json") is False

        # Case 3: valid zlib + valid JSON, but not a list
        db.put_zip_cache_entry(
            "corrupt_type",
            fingerprint="fp3",
            entry_count=1,
            entries_blob=zlib.compress(b'{"not":"a list"}'),
        )
        assert cache.get_entries("corrupt_type", fingerprint="fp3") is None
        assert db.has_zip_cache_entry("corrupt_type") is False

        st = cache.stats()
        assert st["misses"] == 3
        assert st["invalidations"] == 3
    finally:
        db.close()


def test_mutable_mapping_backwards_compatibility_and_purge(tmp_path: Path):
    db = Database(tmp_path / "compat.db")
    try:
        cache = ZipDirectoryCache(db=db, max_l1_items=2, max_db_rows=10)
        e1 = _sample_entries("one")
        e2 = _sample_entries("two")
        e3 = _sample_entries("three")

        # __setitem__, __contains__, __getitem__, get
        cache["k1"] = e1
        cache["k2"] = e2
        cache["k3"] = e3  # k1 evicted from L1, remains in L2

        assert "k1" in cache
        assert "k2" in cache
        assert "k3" in cache
        assert "missing" not in cache
        assert len(cache) == 3
        assert set(cache) == {"k1", "k2", "k3"}

        assert cache["k1"] == e1
        assert cache.get("k2") == e2
        assert cache.get("missing") is None
        assert cache.get("missing", []) == []

        with pytest.raises(KeyError):
            _ = cache["missing"]

        # pop with and without default
        assert cache.pop("k1", None) == e1
        assert "k1" not in cache
        assert cache.pop("k1", None) is None
        with pytest.raises(KeyError):
            cache.pop("k1")

        # delitem
        del cache["k2"]
        assert "k2" not in cache
        with pytest.raises(KeyError):
            del cache["k2"]

        # purge / clear
        cache["k4"] = e1
        res = cache.purge()
        assert res["l1_cleared"] >= 1
        assert res["l2_cleared"] == 2
        assert len(cache) == 0

        cache["k5"] = e2
        cache.clear()
        assert len(cache) == 0
        assert db.get_zip_cache_db_stats() == {"entries": 0, "total_bytes": 0}

        # stats keys and hit_rate_pct
        st = cache.stats()
        expected_keys = {
            "l1_entries",
            "l1_max_entries",
            "l2_entries",
            "l2_max_entries",
            "l2_bytes",
            "l2_max_bytes",
            "l1_hits",
            "l2_hits",
            "hits",
            "misses",
            "hit_rate_pct",
            "l1_evictions",
            "l2_evictions",
            "invalidations",
        }
        assert expected_keys.issubset(st.keys())
    finally:
        db.close()


def test_thread_safety_concurrent_reads_and_writes(tmp_path: Path):
    db = Database(tmp_path / "threads.db")
    try:
        cache = ZipDirectoryCache(db=db, max_l1_items=8, max_db_rows=20)
        errors: list[Exception] = []

        def worker(tid: int) -> None:
            try:
                for i in range(25):
                    mid = f"item_{(tid * 5 + i) % 15}"
                    entries = _sample_entries(mid, count=2)
                    cache.set_entries(mid, entries, fingerprint=f"fp_{mid}")
                    got = cache.get_entries(mid, fingerprint=f"fp_{mid}")
                    assert got is None or got == entries
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        assert cache.stats()["l1_entries"] <= 8
        assert cache.stats()["l2_entries"] <= 20
    finally:
        db.close()
