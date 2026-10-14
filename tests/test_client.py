"""Behaviour of the blocking client against the in-process fake cluster.

The blocking client is generated from the asyncio one, so these tests cover
the shared logic; ``test_async_client.py`` checks the asyncio surface.
"""

from __future__ import annotations

import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path

import pytest

import tikv
from tikv._async import _client as async_client_module
from tikv._sync import _client as client_module

from .fake_cluster import FakeCluster


class TestBasics:
    def test_put_then_get(self, client: tikv.RawClient) -> None:
        client.put(b"k", b"v")
        assert client.get(b"k") == b"v"

    def test_get_missing_key_returns_none(self, client: tikv.RawClient) -> None:
        assert client.get(b"missing") is None

    def test_empty_value_is_not_missing(self, client: tikv.RawClient) -> None:
        client.put(b"k", b"")
        assert client.get(b"k") == b""

    def test_put_overwrites(self, client: tikv.RawClient) -> None:
        client.put(b"k", b"1")
        client.put(b"k", b"2")
        assert client.get(b"k") == b"2"

    def test_delete(self, client: tikv.RawClient) -> None:
        client.put(b"k", b"v")
        client.delete(b"k")
        client.delete(b"never-existed")
        assert client.get(b"k") is None

    def test_accepts_bytes_like(self, client: tikv.RawClient) -> None:
        client.put(bytearray(b"k"), memoryview(b"v"))
        assert client.get(b"k") == b"v"

    @pytest.mark.parametrize(
        "call", [lambda c: c.get("k"), lambda c: c.put(b"k", "v"), lambda c: c.delete(1)]
    )
    def test_rejects_non_bytes(self, client: tikv.RawClient, call) -> None:
        with pytest.raises(TypeError, match="must be bytes"):
            call(client)

    def test_repr(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        assert repr(client) == f"RawClient([{cluster.address!r}], timeout=5, atomic=False)"


class TestBatches:
    def test_get_many_skips_missing_and_keeps_order(self, client: tikv.RawClient) -> None:
        client.put_many({b"b": b"2", b"a": b"1", b"c": b"3"})
        assert list(client.get_many([b"c", b"missing", b"a", b"c"]).items()) == [(b"c", b"3"), (b"a", b"1")]

    def test_put_many_accepts_pairs(self, client: tikv.RawClient) -> None:
        client.put_many((bytes([i]), b"v") for i in range(10))
        assert len(client.get_many(bytes([i]) for i in range(10))) == 10

    def test_empty_batches_are_no_ops(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        client.put_many({})
        client.delete_many([])
        assert client.get_many([]) == {}
        assert not cluster.calls

    def test_batches_span_regions(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        cluster.split(b"k1", b"k3", b"k5", b"k7")
        items = {f"k{i}-{j:04}".encode(): b"x" * 100 for i in range(9) for j in range(300)}
        client.put_many(items)
        assert client.get_many(items) == items
        assert cluster.calls["RawBatchPut"] >= cluster.region_count()
        client.delete_many(list(items)[::2])
        assert len(client.get_many(items)) == len(items) // 2

    def test_large_batches_are_chunked(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        items = {f"{i:05}".encode(): b"" for i in range(client_module.BATCH_MAX_KEYS * 3)}
        client.put_many(items)
        assert cluster.calls["RawBatchPut"] == 3
        big = {f"big{i}".encode(): b"x" * (client_module.BATCH_MAX_BYTES // 2 - 100) for i in range(4)}
        client.put_many(big)
        assert cluster.calls["RawBatchPut"] == 3 + 2

    def test_put_many_with_ttl(self, client: tikv.RawClient) -> None:
        client.put_many({b"a": b"1", b"b": b"2"}, ttl=100)
        assert 99 <= (client.ttl(b"a") or 0) <= 100


class TestScan:
    @pytest.fixture(autouse=True)
    def _data(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        cluster.split(b"b", b"d", b"f")
        client.put_many({bytes([c]) + bytes([n]): bytes([n]) for c in b"abcdefg" for n in b"12"})

    def keys(self, client: tikv.RawClient, *args, **kwargs) -> list[bytes]:
        return [k for k, _ in client.scan(*args, **kwargs)]

    def test_everything(self, client: tikv.RawClient) -> None:
        assert self.keys(client) == sorted(f"{c}{n}".encode() for c in "abcdefg" for n in "12")

    def test_bounds(self, client: tikv.RawClient) -> None:
        assert self.keys(client, b"b2", b"d2") == [b"b2", b"c1", b"c2", b"d1"]
        assert self.keys(client, b"f") == [b"f1", b"f2", b"g1", b"g2"]
        assert self.keys(client, end=b"b") == [b"a1", b"a2"]

    def test_values(self, client: tikv.RawClient) -> None:
        assert list(client.scan(b"c", b"d")) == [(b"c1", b"1"), (b"c2", b"2")]

    def test_prefix(self, client: tikv.RawClient) -> None:
        assert self.keys(client, prefix=b"d") == [b"d1", b"d2"]
        with pytest.raises(ValueError, match="either prefix or start/end"):
            list(client.scan(b"a", prefix=b"d"))

    def test_limit(self, client: tikv.RawClient) -> None:
        assert self.keys(client, b"a2", limit=5) == [b"a2", b"b1", b"b2", b"c1", b"c2"]
        assert self.keys(client, limit=0) == []
        with pytest.raises(ValueError, match="limit"):
            list(client.scan(limit=-1))

    def test_reverse(self, client: tikv.RawClient) -> None:
        assert self.keys(client, b"b2", b"d2", reverse=True) == [b"d1", b"c2", b"c1", b"b2"]
        assert self.keys(client, b"f", reverse=True) == [b"g2", b"g1", b"f2", b"f1"]
        assert self.keys(client, reverse=True, limit=3) == [b"g2", b"g1", b"f2"]
        assert self.keys(client, prefix=b"a", reverse=True) == [b"a2", b"a1"]

    def test_reverse_unbounded_reaches_largest_keys(self, client: tikv.RawClient) -> None:
        client.put(b"\xff\xff\xff", b"max")
        assert next(iter(client.scan(reverse=True))) == (b"\xff\xff\xff", b"max")

    def test_empty_ranges(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        calls = sum(cluster.calls.values())
        assert self.keys(client, b"d", b"c") == []
        assert self.keys(client, b"d", b"d") == []
        assert self.keys(client, b"a", b"") == []  # b"" is a key, not "unbounded"
        assert sum(cluster.calls.values()) == calls

    def test_scan_keys(self, client: tikv.RawClient) -> None:
        assert list(client.scan_keys(prefix=b"g")) == [b"g1", b"g2"]
        assert list(client.scan_keys(b"a", b"b", reverse=True)) == [b"a2", b"a1"]

    def test_pages_within_a_region(self, client: tikv.RawClient, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(client_module, "SCAN_PAGE_SIZE", 3)
        assert self.keys(client) == sorted(f"{c}{n}".encode() for c in "abcdefg" for n in "12")
        assert self.keys(client, reverse=True) == sorted(
            (f"{c}{n}".encode() for c in "abcdefg" for n in "12"), reverse=True
        )

    def test_full_pages_ending_at_the_bounds(
        self, client: tikv.RawClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(client_module, "SCAN_PAGE_SIZE", 2)
        client.put(b"", b"empty key")
        assert list(client.scan_keys(end=b"a2", reverse=True)) == [b"a1", b""]
        assert list(client.scan_keys(b"a1", b"b1")) == [b"a1", b"a2"]
        assert list(client.scan_keys(b"a2", b"b2", reverse=True)) == [b"b1", b"a2"]

    def test_is_lazy(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        scan = client.scan()
        assert cluster.calls["RawScan"] == 0
        next(scan)
        assert cluster.calls["RawScan"] == 1


class TestDeleteRange:
    def test_deletes_across_regions(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        cluster.split(b"b", b"c")
        client.put_many({b"a": b"", b"ab": b"", b"b": b"", b"bz": b"", b"c": b"", b"d": b""})
        client.delete_range(b"ab", b"c")
        assert list(client.scan_keys()) == [b"a", b"c", b"d"]

    def test_open_ended(self, client: tikv.RawClient) -> None:
        client.put_many({b"a": b"", b"b": b"", b"\xff": b""})
        client.delete_range(b"b")
        assert list(client.scan_keys()) == [b"a"]

    def test_prefix(self, client: tikv.RawClient) -> None:
        client.put_many({b"user:1": b"", b"user:2": b"", b"users": b""})
        client.delete_range(prefix=b"user:")
        assert list(client.scan_keys()) == [b"users"]

    def test_guards_against_wiping_everything_by_accident(self, client: tikv.RawClient) -> None:
        client.put(b"k", b"v")
        with pytest.raises(TypeError, match="requires start or prefix"):
            client.delete_range()
        with pytest.raises(ValueError, match="empty prefix"):
            client.delete_range(prefix=b"")
        assert client.get(b"k") == b"v"
        client.delete_range(b"")
        assert client.get(b"k") is None


class TestChecksum:
    def test_combines_regions(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        items = {f"k{i:03}".encode(): b"v" * i for i in range(100)}
        client.put_many(items)
        whole = client.checksum(prefix=b"k")
        assert whole.keys == 100
        assert whole.size == sum(len(k) + len(v) for k, v in items.items())
        cluster.split(b"k030", b"k060")
        assert client.checksum(prefix=b"k") == whole
        first, rest = client.checksum(end=b"k050"), client.checksum(b"k050")
        assert first.crc64_xor ^ rest.crc64_xor == whole.crc64_xor
        assert first.keys + rest.keys == 100

    def test_changes_with_data(self, client: tikv.RawClient) -> None:
        client.put(b"k", b"1")
        before = client.checksum()
        client.put(b"k", b"2")
        assert client.checksum() != before
        assert client.checksum(b"z", b"a") == (0, 0, 0)


class TestTTL:
    def test_ttl_of_key_without_expiry_is_none(self, client: tikv.RawClient) -> None:
        client.put(b"k", b"v")
        assert client.ttl(b"k") is None

    def test_ttl_counts_down(self, client: tikv.RawClient) -> None:
        client.put(b"k", b"v", ttl=timedelta(minutes=1))
        assert client.ttl(b"k") == 60

    def test_expired_keys_disappear(self, client: tikv.RawClient) -> None:
        client.put(b"k", b"v", ttl=0.01)  # rounds up to one second
        assert client.get(b"k") == b"v"
        time.sleep(1.05)
        assert client.get(b"k") is None

    def test_ttl_of_missing_key_raises_key_error(self, client: tikv.RawClient) -> None:
        with pytest.raises(KeyError):
            client.ttl(b"missing")

    @pytest.mark.parametrize("ttl", [0, -1, float("inf"), timedelta(0)])
    def test_rejects_non_positive_ttl(self, client: tikv.RawClient, ttl) -> None:
        with pytest.raises(ValueError, match="positive"):
            client.put(b"k", b"v", ttl=ttl)

    @pytest.mark.parametrize("ttl", ["60", True])
    def test_rejects_wrong_ttl_type(self, client: tikv.RawClient, ttl) -> None:
        with pytest.raises(TypeError, match="ttl"):
            client.put(b"k", b"v", ttl=ttl)


class TestCompareAndSwap:
    def test_requires_atomic_mode(self, client: tikv.RawClient) -> None:
        with pytest.raises(tikv.TiKVError, match="atomic=True"):
            client.compare_and_swap(b"k", None, b"v")

    def test_swaps_when_expected_matches(self, atomic_client: tikv.RawClient) -> None:
        assert atomic_client.compare_and_swap(b"k", None, b"1") == (True, None)
        assert atomic_client.compare_and_swap(b"k", None, b"2") == (False, b"1")
        assert atomic_client.compare_and_swap(b"k", b"0", b"2") == (False, b"1")
        swapped, previous = atomic_client.compare_and_swap(b"k", b"1", b"2")
        assert swapped
        assert previous == b"1"
        assert atomic_client.get(b"k") == b"2"

    def test_result_type(self, atomic_client: tikv.RawClient) -> None:
        result = atomic_client.compare_and_swap(b"k", None, b"1")
        assert result.swapped
        assert result.previous is None

    def test_atomic_mode_rejects_delete_range(self, atomic_client: tikv.RawClient) -> None:
        with pytest.raises(tikv.TiKVError, match="atomic mode"):
            atomic_client.delete_range(b"a", b"b")


def test_sync_client_is_generated_from_async_one() -> None:
    assert client_module.BATCH_MAX_KEYS == async_client_module.BATCH_MAX_KEYS
    script = Path(__file__).parent.parent / "scripts" / "unasync.py"
    result = subprocess.run([sys.executable, str(script), "--check"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_one_client_shared_by_threads(client: tikv.RawClient, cluster: FakeCluster) -> None:
    cluster.split(*(f"{i:02}".encode() for i in range(0, 100, 10)))

    def work(n: int) -> None:
        for i in range(25):
            key = f"{n:02}-{i}".encode()
            client.put(key, key)
            assert client.get(key) == key
        assert len(list(client.scan_keys(prefix=f"{n:02}-".encode()))) == 25

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(work, range(100)))
