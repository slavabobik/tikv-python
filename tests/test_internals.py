"""Unit tests for the pure building blocks: key arithmetic, cache, backoff."""

from __future__ import annotations

import pytest

import tikv
from tikv._backoff import Backoff, Wait
from tikv._keys import chunked, key_range, min_end, prefix_end
from tikv._proto import metapb_pb2 as metapb
from tikv._region import Region, RegionCache, make_region


def region(region_id: int, start: bytes, end: bytes, version: int = 1) -> Region:
    meta = metapb.Region(
        id=region_id,
        start_key=start,
        end_key=end,
        region_epoch=metapb.RegionEpoch(version=version),
        peers=[metapb.Peer(id=region_id * 10 + s, store_id=s) for s in (1, 2, 3)],
    )
    return make_region(meta, meta.peers[0])


@pytest.mark.parametrize(
    ("prefix", "end"),
    [(b"a", b"b"), (b"a\xff", b"b"), (b"a\xfe", b"a\xff"), (b"\xff\xff", b""), (b"", b"")],
)
def test_prefix_end(prefix: bytes, end: bytes) -> None:
    assert prefix_end(prefix) == end


def test_key_range() -> None:
    assert key_range(None, None, None) == (b"", b"")
    assert key_range(b"a", None, None) == (b"a", b"")
    assert key_range(b"b", b"a", None) is None
    assert key_range(None, None, b"user:") == (b"user:", b"user;")


def test_min_end() -> None:
    assert min_end(b"", b"x") == b"x"
    assert min_end(b"x", b"") == b"x"
    assert min_end(b"a", b"b") == b"a"


def test_chunked() -> None:
    assert list(chunked([1, 2, 3, 4, 5], max_count=2)) == [[1, 2], [3, 4], [5]]
    items = [b"aa", b"bbbb", b"c", b"dddddd"]
    assert list(chunked(items, max_count=10, max_bytes=5, size=len)) == [
        [b"aa"],
        [b"bbbb", b"c"],
        [b"dddddd"],
    ]


class TestRegionCache:
    def test_find(self) -> None:
        cache = RegionCache()
        for r in (region(1, b"", b"g"), region(2, b"g", b"p"), region(3, b"p", b"")):
            cache.put(r)
        assert cache.find(b"") is not None
        assert [cache.find(k).id for k in (b"a", b"g", b"o", b"p", b"\xff")] == [1, 2, 2, 3, 3]  # type: ignore[union-attr]
        assert [cache.find_end(k).id for k in (b"a", b"g", b"h", b"p", b"\xff")] == [1, 1, 2, 2, 3]  # type: ignore[union-attr]
        assert cache.find_end(b"") is None

    def test_put_evicts_overlapping_regions(self) -> None:
        cache = RegionCache()
        cache.put(region(1, b"a", b"m"))
        cache.put(region(2, b"m", b"z"))
        cache.put(region(3, b"f", b"p", version=2))  # after a merge + split
        assert cache.find(b"b") is None
        assert cache.find(b"g").id == 3  # type: ignore[union-attr]
        assert cache.find(b"q") is None

    def test_gaps_are_misses(self) -> None:
        cache = RegionCache()
        cache.put(region(1, b"a", b"c"))
        cache.put(region(2, b"x", b""))
        assert cache.find(b"d") is None
        assert cache.find_end(b"x") is None

    def test_invalidate_and_update_leader(self) -> None:
        cache = RegionCache()
        r = region(1, b"", b"")
        cache.put(r)
        assert cache.update_leader(r, metapb.Peer(store_id=3))
        assert cache.find(b"k").leader.store_id == 3  # type: ignore[union-attr]
        assert not cache.update_leader(r, metapb.Peer(store_id=9))  # not a peer: drop it
        assert cache.find(b"k") is None


class TestBackoff:
    def test_delays_grow_and_respect_cap(self) -> None:
        backoff = Backoff(timeout=1000)
        delays = [backoff.delay(Wait.TIKV_RPC, RuntimeError()) for _ in range(10)]
        assert delays[0] <= 0.1
        assert max(delays) <= 2.0
        assert delays[-1] >= 1.0

    def test_server_hint_is_a_minimum(self) -> None:
        assert Backoff(timeout=10).delay(Wait.SERVER_BUSY, RuntimeError(), hint=5) == 5

    def test_raises_timeout_with_cause(self) -> None:
        cause = RuntimeError("boom")
        with pytest.raises(tikv.TimeoutError, match=r"within 0\.001s: boom") as caught:
            Backoff(timeout=0.001).delay(Wait.SERVER_BUSY, cause)
        assert caught.value.__cause__ is cause
