from __future__ import annotations

import asyncio
import os
from pathlib import Path
import time
import warnings

import pytest

from app.stream_cache import (
    StreamFetchCoordinator,
    TelegramBlockCache,
    TwoTierBlockCache,
)


def test_l1_ram_only_and_legacy_stats_compatibility():
    assert TelegramBlockCache is TwoTierBlockCache

    cache = TelegramBlockCache(max_bytes=100, block_size=4096)
    assert cache.enabled is True
    assert cache.l1_enabled is True
    assert cache.l2_enabled is False
    assert cache.max_bytes == 100
    assert cache.block_size == 4096

    cache.put(("ns", 1), 0, b"a" * 60)
    assert len(cache) == 1
    assert cache.total_bytes == 60
    assert cache.has_block(("ns", 1), 0) is True

    # Hit and miss matching test_worker_telemetry_and_routing.py
    assert cache.get_span(("ns", 1), 0, 10) == b"a" * 11
    assert cache.get_span(("ns", 2), 0, 10) is None

    # Adding second 60-byte block evicts LRU offset 0
    cache.put(("ns", 1), 60, b"b" * 60)
    assert len(cache) == 1
    assert cache.total_bytes == 60
    assert cache.get_span(("ns", 1), 0, 5) is None  # second miss

    # Reset and verify exact legacy stats values on 1 hit, 1 miss, 1 eviction
    cache2 = TelegramBlockCache(max_bytes=100, block_size=4096)
    cache2.configure(max_bytes=100, block_size=4096)
    cache2.put(("ns", 1), 0, b"a" * 60)
    assert cache2.get_span(("ns", 1), 0, 10) == b"a" * 11
    assert cache2.get_span(("ns", 2), 0, 10) is None
    cache2.put(("ns", 1), 60, b"b" * 60)

    st = cache2.stats()
    assert st["enabled"] is True
    assert st["used_bytes"] == 60
    assert st["max_bytes"] == 100
    assert st["utilization_pct"] == 60.0
    assert st["entries"] == 1
    assert st["block_size"] == 4096
    assert st["hits"] == 1
    assert st["misses"] == 1
    assert st["evictions"] == 1
    assert st["hit_rate_pct"] == 50.0
    assert st["l1"]["enabled"] is True
    assert st["l2"]["enabled"] is False
    assert st["l2"]["dir"] is None


def test_l2_disk_atomic_writes_persistence_and_l1_promotion(tmp_path: Path):
    disk_dir = tmp_path / "l2_cache"
    ns = ("-100123456", 9001)
    block0 = b"0" * 1024
    block1 = b"1" * 1024

    cache = TwoTierBlockCache(
        max_bytes=1024,  # L1 holds only 1 block
        block_size=1024,
        disk_max_bytes=4096,
        disk_dir=disk_dir,
    )
    assert cache.l1_enabled is True
    assert cache.l2_enabled is True

    cache.put(ns, 0, block0)
    cache.put(ns, 1024, block1)  # evicts block0 from L1, both remain in L2

    assert len(cache) == 1
    assert cache.get_disk_footprint() == (2, 2048)

    # Verify atomic writes left no .tmp_* files on disk
    all_files = [p for p in disk_dir.rglob("*") if p.is_file()]
    assert len(all_files) == 2
    assert not any(p.name.startswith(".tmp_") for p in all_files)
    assert all(p.name.endswith(".blk") for p in all_files)

    # Simulate process restart with a brand new TwoTierBlockCache instance
    restarted = TwoTierBlockCache(
        max_bytes=2048,
        block_size=1024,
        disk_max_bytes=4096,
        disk_dir=disk_dir,
    )
    assert len(restarted) == 0  # L1 starts cold
    assert restarted.get_disk_footprint() == (2, 2048)
    assert restarted.has_block(ns, 0) is True
    assert restarted.has_block(ns, 1024) is True

    # Reading via get_span hits L2 and promotes block0 into L1
    assert restarted.get_span(ns, 100, 199) == block0[100:200]
    assert len(restarted) == 1
    st = restarted.stats()
    assert st["l1"]["misses"] == 1
    assert st["l2"]["hits"] == 1
    assert st["hits"] == 1

    # Subsequent read hits L1 directly
    assert restarted.get_span(ns, 200, 299) == block0[200:300]
    st2 = restarted.stats()
    assert st2["l1"]["hits"] == 1
    assert st2["l2"]["hits"] == 1

    # Async read of block1 hits L2 via asyncio.to_thread and promotes to L1
    data, tier = asyncio.run(restarted.get_block_async(ns, 1024))
    assert tier == "l2"
    assert data == block1
    assert len(restarted) == 2

    # Second async read hits L1 fast-path
    data2, tier2 = asyncio.run(restarted.get_block_async(ns, 1024))
    assert tier2 == "l1"
    assert data2 == block1


def test_l2_crash_safe_reconciliation_and_cap_enforcement(tmp_path: Path):
    disk_dir = tmp_path / "l2_reconcile"
    # Pre-populate cache with 3 blocks (3000 bytes)
    c1 = TwoTierBlockCache(max_bytes=0, block_size=4096, disk_max_bytes=5000, disk_dir=disk_dir)
    ns = ("chat_a", 1)
    c1.put(ns, 0, b"a" * 1000)
    c1.put(ns, 1000, b"b" * 1000)
    c1.put(ns, 2000, b"c" * 1000)

    # Set explicit mtimes so offset 0 is oldest, offset 1000 is middle, offset 2000 is newest
    blk_files = sorted(disk_dir.rglob("*.blk"), key=lambda p: p.name)
    assert len(blk_files) == 3
    base_ns = time.time_ns() - 10_000_000_000
    for idx, fpath in enumerate(blk_files):
        os.utime(fpath, ns=(base_ns + idx * 1_000_000_000, base_ns + idx * 1_000_000_000))

    # Plant stray crashed .tmp_* files and a zero-byte .blk file
    shard = blk_files[0].parent
    stray_tmp = shard / ".tmp_deadbeef1234.blk"
    stray_tmp.write_bytes(b"partial write")
    zero_blk = shard / f"{'0' * 24}_{9999:016x}.blk"
    zero_blk.write_bytes(b"")

    # Restart with smaller disk_max_bytes=2000 -> oldest (offset 0) must be evicted!
    c2 = TwoTierBlockCache(max_bytes=4096, block_size=4096, disk_max_bytes=2000, disk_dir=disk_dir)
    assert not stray_tmp.exists()
    assert not zero_blk.exists()
    assert c2.get_disk_footprint() == (2, 2000)
    assert c2.has_block(ns, 0) is False
    assert c2.has_block(ns, 1000) is True
    assert c2.has_block(ns, 2000) is True
    assert c2.stats()["l2"]["evictions"] == 1


def test_l2_ephemeral_namespace_guard_invalidate_purge_and_configure(tmp_path: Path):
    disk_dir = tmp_path / "l2_guard"
    cache = TwoTierBlockCache(
        max_bytes=8192,
        block_size=4096,
        disk_max_bytes=16384,
        disk_dir=disk_dir,
    )

    # Ephemeral ("media", id) must stay RAM-only
    eph_ns = ("media", 123456789)
    cache.put(eph_ns, 0, b"ephemeral" * 10)
    assert cache.has_block(eph_ns, 0) is True
    assert cache.get_disk_footprint() == (0, 0)
    assert list(disk_dir.rglob("*.blk")) == []

    # Persistent namespaces go to both RAM and disk
    p_ns1 = ("-100999", 10)
    p_ns2 = ("-100999", 11)
    cache.put(p_ns1, 0, b"x" * 512)
    cache.put(p_ns1, 512, b"y" * 512)
    cache.put(p_ns2, 0, b"z" * 512)
    assert cache.get_disk_footprint() == (3, 1536)
    assert len(cache) == 4  # 1 ephemeral + 3 persistent

    # Invalidate removes L1 and unlinks L2 files for p_ns1 only
    removed = cache.invalidate(p_ns1)
    assert removed == 4  # 2 in L1 + 2 in L2
    assert cache.has_block(p_ns1, 0) is False
    assert cache.get_disk_footprint() == (1, 512)
    assert cache.has_block(p_ns2, 0) is True

    # Selective purge: clear_ram=True, clear_disk=False
    res_ram = cache.purge(clear_ram=True, clear_disk=False)
    assert res_ram["ram_entries_cleared"] == 2
    assert res_ram["disk_entries_cleared"] == 0
    assert len(cache) == 0
    assert cache.get_disk_footprint() == (1, 512)

    # Selective purge: clear_ram=False, clear_disk=True
    res_disk = cache.purge(clear_ram=False, clear_disk=True)
    assert res_disk["ram_entries_cleared"] == 0
    assert res_disk["disk_entries_cleared"] == 1
    assert res_disk["disk_bytes_cleared"] == 512
    assert cache.get_disk_footprint() == (0, 0)

    # Corrupt/missing file on disk falls through cleanly to miss
    cache.put(p_ns2, 0, b"k" * 512)
    cache.purge(clear_ram=True, clear_disk=False)
    for f in disk_dir.rglob("*.blk"):
        f.unlink()
    assert cache.get_span(p_ns2, 0, 10) is None
    assert cache.get_disk_footprint() == (0, 0)

    # Lowering disk_max_bytes via configure evicts immediately
    cache.put(p_ns2, 0, b"m" * 512)
    cache.put(p_ns2, 512, b"n" * 512)
    assert cache.get_disk_footprint() == (2, 1024)
    cache.configure(max_bytes=8192, block_size=4096, disk_max_bytes=512)
    assert cache.get_disk_footprint() == (1, 512)

    # Disabling L2 with non-existent dir does not raise
    cache.configure(max_bytes=8192, block_size=4096, disk_max_bytes=0, disk_dir=tmp_path / "missing_dir")
    assert cache.l2_enabled is False
    assert cache.get_disk_footprint() == (0, 0)


def test_single_flight_coalescing_and_cancellation():
    async def _run():
        cache = TwoTierBlockCache(max_bytes=64 * 1024, block_size=4096)
        coord = StreamFetchCoordinator(cache)
        ns = ("chat", 1)

        # 1) 5 concurrent callers -> 1 fetch, coalesced_waits == 4
        fetch_calls = 0
        gate = asyncio.Event()

        async def fetch_once() -> bytes:
            nonlocal fetch_calls
            fetch_calls += 1
            await gate.wait()
            return b"payload-block-0"

        async def release_soon():
            await asyncio.sleep(0.02)
            gate.set()

        releaser = asyncio.create_task(release_soon())
        results = await asyncio.gather(
            *(coord.fetch_block(ns, 0, fetch_once) for _ in range(5))
        )
        await releaser

        assert fetch_calls == 1
        assert all(data == b"payload-block-0" for data, _src in results)
        sources = [src for _data, src in results]
        assert sources.count("fetch") == 1
        assert sources.count("coalesced") == 4
        assert coord.stats()["coalesced_waits"] == 4
        assert cache.has_block(ns, 0) is True

        # 2) Waiter 1 cancels while Waiter 2 is still waiting -> task keeps running & Waiter 2 succeeds
        gate2 = asyncio.Event()
        fetch2_calls = 0

        async def fetch_two() -> bytes:
            nonlocal fetch2_calls
            fetch2_calls += 1
            await gate2.wait()
            return b"payload-block-1"

        t1 = asyncio.create_task(coord.fetch_block(ns, 4096, fetch_two))
        t2 = asyncio.create_task(coord.fetch_block(ns, 4096, fetch_two))
        await asyncio.sleep(0.01)

        t1.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t1

        gate2.set()
        data2, src2 = await t2
        assert fetch2_calls == 1
        assert data2 == b"payload-block-1"
        assert src2 == "coalesced"

        # 3) Sole foreground waiter cancels -> underlying fetch is cancelled and cleaned up
        underlying_cancelled = False
        started = asyncio.Event()

        async def fetch_three() -> bytes:
            nonlocal underlying_cancelled
            started.set()
            try:
                await asyncio.sleep(10)
                return b"never"
            except asyncio.CancelledError:
                underlying_cancelled = True
                raise

        t_sole = asyncio.create_task(coord.fetch_block(ns, 8192, fetch_three))
        await started.wait()
        t_sole.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t_sole

        assert underlying_cancelled is True
        assert coord._inflight == {}
        await coord.stop()

    asyncio.run(_run())


def test_prefetch_heuristics_hits_concurrency_errors_and_stop():
    async def _run():
        block_size = 4096
        cache = TwoTierBlockCache(max_bytes=128 * 1024, block_size=block_size)
        coord = StreamFetchCoordinator(cache, prefetch_blocks=2, max_prefetch_concurrency=2)
        ns = ("chat", 77)

        # Single-block probe does NOT trigger prefetch
        assert coord.should_prefetch(ns, 0, 1023, block_size) is False
        # Contiguous read still inside block 0 does NOT trigger prefetch
        assert coord.should_prefetch(ns, 1024, 2047, block_size) is False
        # Contiguous stride crossing into block 1 DOES trigger prefetch
        assert coord.should_prefetch(ns, 4096, 8191, block_size) is True
        # Multi-block single request on fresh namespace DOES trigger prefetch
        ns_multi = ("chat", 78)
        assert coord.should_prefetch(ns_multi, 0, block_size + 100, block_size) is True

        # Completed prefetch hit tracking via note_cache_hit (counted once)
        async def ok_fetch() -> bytes:
            await asyncio.sleep(0.01)
            return b"P" * block_size

        assert coord.schedule_prefetch(ns, block_size * 2, ok_fetch) is True
        await asyncio.sleep(0.05)
        assert cache.has_block(ns, block_size * 2) is True
        assert coord.note_cache_hit(ns, block_size * 2) is True
        assert coord.note_cache_hit(ns, block_size * 2) is False
        assert coord.stats()["prefetch"]["hits"] == 1
        assert coord.stats()["prefetch"]["completed"] == 1

        # Foreground caller joining an in-flight prefetch increments hits and prevents cancel
        gate = asyncio.Event()

        async def slow_prefetch() -> bytes:
            await gate.wait()
            return b"Q" * block_size

        assert coord.schedule_prefetch(ns, block_size * 3, slow_prefetch) is True
        fg_task = asyncio.create_task(coord.fetch_block(ns, block_size * 3, slow_prefetch))
        await asyncio.sleep(0.01)

        # Cancel prefetch for namespace must NOT cancel the promoted prefetch!
        assert coord.cancel_prefetch_for_namespace(ns) == 0
        gate.set()
        fg_data, fg_src = await fg_task
        assert fg_data == b"Q" * block_size
        assert fg_src == "prefetch_hit"
        assert coord.stats()["prefetch"]["hits"] == 2

        # Seek jump cancels unpromoted prefetch tasks for namespace
        gate_seek = asyncio.Event()

        async def hang_fetch() -> bytes:
            await gate_seek.wait()
            return b"H" * block_size

        assert coord.schedule_prefetch(ns, block_size * 4, hang_fetch) is True
        await asyncio.sleep(0.01)
        # Non-contiguous seek jump triggers cancellation
        assert coord.should_prefetch(ns, block_size * 20, block_size * 20 + 500, block_size) is False
        await asyncio.sleep(0.02)
        assert coord.stats()["prefetch"]["active_tasks"] == 0

        # EOF guard prevents prefetch at/past EOF
        coord.record_eof(ns, block_size * 5)
        assert coord.schedule_prefetch(ns, block_size * 5, ok_fetch) is False
        assert coord.schedule_prefetch(ns, block_size * 6, ok_fetch) is False
        coord.clear_eof(ns)

        # Concurrency cap strictly drops excess tasks & can_run_fn False drops cleanly
        cap_gate = asyncio.Event()

        async def gated_fetch() -> bytes:
            await cap_gate.wait()
            return b"C" * block_size

        assert coord.schedule_prefetch(ns, block_size * 10, gated_fetch) is True
        assert coord.schedule_background_warm("warm-1", gated_fetch, namespace=ns) is True
        # Third background task exceeds max_prefetch_concurrency=2 -> dropped
        assert coord.schedule_prefetch(ns, block_size * 11, gated_fetch) is False
        assert coord.stats()["prefetch"]["drops"] >= 1
        cap_gate.set()
        await asyncio.sleep(0.03)

        # can_run_fn returning False increments drops
        assert coord.schedule_prefetch(ns, block_size * 12, ok_fetch, can_run_fn=lambda: False) is False

        # Exception / FloodWaitError calls on_error without unhandled task errors
        captured_errors: list[Exception] = []

        class FakeFloodWaitError(Exception):
            pass

        async def fail_fetch() -> bytes:
            raise FakeFloodWaitError("wait 30s")

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            assert (
                coord.schedule_prefetch(
                    ns,
                    block_size * 13,
                    fail_fetch,
                    on_error=captured_errors.append,
                )
                is True
            )
            await asyncio.sleep(0.03)
            assert len(captured_errors) == 1
            assert isinstance(captured_errors[0], FakeFloodWaitError)
            assert coord.stats()["prefetch"]["errors"] == 1
            assert not any("never retrieved" in str(w.message) for w in caught)

        # stop() cancels and awaits all active prefetch, warm, and inflight tasks
        stop_gate = asyncio.Event()

        async def infinite_task() -> bytes:
            await stop_gate.wait()
            return b"X"

        coord.schedule_prefetch(ns, block_size * 14, infinite_task)
        coord.schedule_background_warm("warm-stop", infinite_task, namespace=ns)
        await asyncio.sleep(0.01)
        assert coord.stats()["prefetch"]["active_tasks"] == 2

        await coord.stop()
        assert coord.stats()["prefetch"]["active_tasks"] == 0
        assert coord._inflight == {}

    asyncio.run(_run())


def test_coordinator_multi_event_loop_asyncio_run_safety():
    cache = TwoTierBlockCache(max_bytes=64 * 1024, block_size=4096)
    coord = StreamFetchCoordinator(cache, prefetch_blocks=2, max_prefetch_concurrency=2)
    ns = ("chat", 555)

    async def phase(offset: int, payload: bytes) -> tuple[bytes, str]:
        async def _fetch() -> bytes:
            await asyncio.sleep(0.005)
            return payload

        coord.schedule_prefetch(ns, offset + 4096, _fetch)
        res = await coord.fetch_block(ns, offset, _fetch)
        await asyncio.sleep(0.02)
        await coord.stop()
        return res

    # Repeated asyncio.run() calls on the SAME coordinator instance must never raise loop-mismatch errors
    data1, src1 = asyncio.run(phase(0, b"loop1"))
    assert (data1, src1) == (b"loop1", "fetch")

    data2, src2 = asyncio.run(phase(8192, b"loop2"))
    assert (data2, src2) == (b"loop2", "fetch")
    assert coord.stats()["prefetch"]["active_tasks"] == 0


def test_adversarial_hardening_cancelled_prefetch_block_size_symlinks_and_shutdown(tmp_path: Path):
    # 1) Block-size change invalidates L1 and L2 mismatched blocks (prevents silent truncation)
    disk_dir = tmp_path / "l2_blksize"
    ns = ("-100777", 42)
    c1 = TwoTierBlockCache(
        max_bytes=64 * 1024,
        block_size=4096,
        disk_max_bytes=64 * 1024,
        disk_dir=disk_dir,
    )
    c1.put(ns, 0, b"a" * 4096)
    assert c1.has_block(ns, 0) is True
    assert c1.get_disk_footprint() == (1, 4096)

    # Plant a symlinked .blk file inside shard dir — must be ignored by reconciliation
    secret_target = tmp_path / "outside_secret.txt"
    secret_target.write_bytes(b"SECRET_DATA")
    blk_files = list(disk_dir.rglob("*.blk"))
    assert len(blk_files) == 1
    symlink_blk = blk_files[0].parent / f"{'f' * 24}_{0:016x}_{8192:08x}.blk"
    symlink_blk.symlink_to(secret_target)

    # Restart with larger block_size=8192 -> old 4096-byte block is purged, symlink is skipped (and target intact)
    c2 = TwoTierBlockCache(
        max_bytes=64 * 1024,
        block_size=8192,
        disk_max_bytes=64 * 1024,
        disk_dir=disk_dir,
    )
    assert c2.has_block(ns, 0) is False
    assert c2.get_disk_footprint() == (0, 0)
    assert secret_target.read_bytes() == b"SECRET_DATA"

    # Live configure() changing block_size clears L1 & purges mismatched L2 blocks
    c2.put(ns, 0, b"b" * 8192)
    assert c2.has_block(ns, 0) is True
    c2.configure(max_bytes=64 * 1024, block_size=16384, disk_max_bytes=64 * 1024, disk_dir=disk_dir)
    assert c2.has_block(ns, 0) is False
    assert len(c2) == 0
    assert c2.get_disk_footprint() == (0, 0)

    # 2) Foreground fetch_block immediately after cancel_prefetch_for_namespace must NOT receive CancelledError
    async def _run_cancel_race():
        cache = TwoTierBlockCache(max_bytes=64 * 1024, block_size=4096)
        coord = StreamFetchCoordinator(cache, prefetch_blocks=2, max_prefetch_concurrency=2)
        gate = asyncio.Event()

        async def slow_prefetch() -> bytes:
            await gate.wait()
            return b"P" * 4096

        async def fast_fg() -> bytes:
            return b"F" * 4096

        assert coord.schedule_prefetch(ns, 4096, slow_prefetch) is True
        # Cancel idle prefetch and IMMEDIATELY (without yielding!) call fetch_block on the same offset
        cancelled = coord.cancel_prefetch_for_namespace(ns)
        assert cancelled == 1
        data, src = await coord.fetch_block(ns, 4096, fast_fg)
        assert data == b"F" * 4096
        assert src == "fetch"

        # 3) configure(max_prefetch_concurrency=...) after stop() on the same loop must NOT clear _shutting_down
        await coord.stop()
        coord.configure(max_prefetch_concurrency=8)
        assert coord.schedule_prefetch(ns, 8192, fast_fg) is False

    asyncio.run(_run_cancel_race())

