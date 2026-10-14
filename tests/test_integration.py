"""End-to-end tests against a real TiKV cluster.

Run with ``TIKV_PD_ENDPOINTS=pd-host:2379 pytest -m integration``.

Every key written lives under a unique per-run prefix and is deleted at the
end, so these tests are safe to point at a shared (non-production) cluster.
They never write outside that prefix.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from collections.abc import Iterator

import pytest

import tikv
from tikv._backoff import Backoff
from tikv._region import Region

ENDPOINTS = os.environ.get("TIKV_PD_ENDPOINTS", "")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not ENDPOINTS, reason="set TIKV_PD_ENDPOINTS to run integration tests"),
]


@pytest.fixture(scope="module")
def prefix() -> Iterator[bytes]:
    prefix = f"__tikv_python_test__/{uuid.uuid4().hex}/".encode()
    yield prefix
    with tikv.RawClient(ENDPOINTS.split(",")) as client:
        client.delete_range(prefix=prefix)
        assert list(client.scan_keys(prefix=prefix)) == []


@pytest.fixture(scope="module")
def client(prefix: bytes) -> Iterator[tikv.RawClient]:
    with tikv.RawClient(ENDPOINTS.split(",")) as client:
        yield client


@pytest.fixture
def ns(prefix: bytes, request: pytest.FixtureRequest) -> bytes:
    """A key namespace private to the current test."""
    return prefix + request.node.name.encode() + b"/"


def test_crud(client: tikv.RawClient, ns: bytes) -> None:
    key = ns + b"key"
    assert client.get(key) is None
    client.put(key, b"value")
    assert client.get(key) == b"value"
    client.put(key, b"other")
    assert client.get(key) == b"other"
    client.delete(key)
    assert client.get(key) is None


def test_binary_and_large_values(client: tikv.RawClient, ns: bytes) -> None:
    binary = bytes(range(256)) * 4
    large = os.urandom(4 * 1024 * 1024)
    client.put(ns + b"\x00\xff", binary)
    client.put(ns + b"large", large)
    assert client.get(ns + b"\x00\xff") == binary
    assert client.get(ns + b"large") == large


def test_empty_value(client: tikv.RawClient, ns: bytes) -> None:
    client.put(ns + b"empty", b"")
    assert client.get(ns + b"empty") == b""


def test_batches(client: tikv.RawClient, ns: bytes) -> None:
    items = {ns + f"{i:06}".encode(): os.urandom(64) for i in range(5000)}
    client.put_many(items)
    assert client.get_many(items) == items
    assert client.get_many([ns + b"missing", *list(items)[:3]]) == dict(list(items.items())[:3])
    client.delete_many(list(items)[1000:])
    assert len(client.get_many(items)) == 1000


def test_scan(client: tikv.RawClient, ns: bytes) -> None:
    items = {ns + f"{i:04}".encode(): f"v{i}".encode() for i in range(1500)}
    client.put_many(items)
    keys = sorted(items)

    assert [k for k, _ in client.scan(prefix=ns)] == keys  # spans several scan pages
    assert dict(client.scan(prefix=ns)) == items
    assert list(client.scan_keys(ns + b"0100", ns + b"0105")) == keys[100:105]
    assert list(client.scan_keys(prefix=ns, limit=3)) == keys[:3]
    assert list(client.scan_keys(prefix=ns, reverse=True, limit=700)) == keys[::-1][:700]
    assert list(client.scan_keys(ns + b"0100", ns + b"0105", reverse=True)) == keys[100:105][::-1]


def test_reverse_scan_without_upper_bound(client: tikv.RawClient, ns: bytes) -> None:
    client.put(ns + b"k", b"v")  # so even an empty cluster has a largest key
    # Fetch the largest key in the cluster, whatever it is.
    largest = list(client.scan_keys(reverse=True, limit=1))
    assert len(largest) == 1
    assert largest[0] >= ns + b"k"
    assert list(client.scan_keys(largest[0], limit=2)) == largest


def test_delete_range(client: tikv.RawClient, ns: bytes) -> None:
    client.put_many({ns + bytes([c]): b"" for c in b"abcdef"})
    client.delete_range(ns + b"b", ns + b"e")
    assert list(client.scan_keys(prefix=ns)) == [ns + b"a", ns + b"e", ns + b"f"]
    client.delete_range(prefix=ns)
    assert list(client.scan_keys(prefix=ns)) == []


def test_checksum(client: tikv.RawClient, ns: bytes) -> None:
    items = {ns + f"{i:04}".encode(): os.urandom(32) for i in range(1000)}
    client.put_many(items)
    checksum = client.checksum(prefix=ns)
    assert checksum.keys == len(items)
    assert checksum.size == sum(len(k) + len(v) for k, v in items.items())
    assert checksum.crc64_xor != 0
    middle = ns + b"0500"
    first, rest = client.checksum(ns, middle), client.checksum(middle, prefix=None, end=ns + b"\xff")
    assert first.crc64_xor ^ rest.crc64_xor == checksum.crc64_xor
    client.put(ns + b"0001", b"changed")
    assert client.checksum(prefix=ns).crc64_xor != checksum.crc64_xor


def test_ttl(client: tikv.RawClient, ns: bytes) -> None:
    client.put(ns + b"forever", b"v")
    client.put(ns + b"short", b"v", ttl=2)
    client.put_many({ns + b"batch": b"v"}, ttl=600)
    assert client.ttl(ns + b"forever") is None
    assert 0 < (client.ttl(ns + b"short") or 0) <= 2
    assert 590 <= (client.ttl(ns + b"batch") or 0) <= 600
    with pytest.raises(KeyError):
        client.ttl(ns + b"missing")
    time.sleep(3.5)
    assert client.get(ns + b"short") is None


def test_compare_and_swap(prefix: bytes, ns: bytes) -> None:
    key = ns + b"cas"
    with tikv.RawClient(ENDPOINTS.split(","), atomic=True) as client:
        assert client.compare_and_swap(key, None, b"1") == (True, None)
        assert client.compare_and_swap(key, None, b"2") == (False, b"1")
        assert client.compare_and_swap(key, b"x", b"2") == (False, b"1")
        assert client.compare_and_swap(key, b"1", b"2") == (True, b"1")
        assert client.get(key) == b"2"
        client.put(key, b"3")
        client.delete(key)
        assert client.compare_and_swap(key, None, b"4", ttl=60).swapped
        assert 0 < (client.ttl(key) or 0) <= 60
        client.delete_many([key])


def test_reads_across_real_region_boundaries(client: tikv.RawClient) -> None:
    """Read-only: batch-get and scan keys sitting on actual region boundaries."""
    regions = asyncio.run(_regions())
    if len(regions) < 2:
        pytest.skip("cluster has a single region")
    boundaries = [r.start for r in regions[1:]][:200]
    probe = [k for b in boundaries for k in (b, b + b"\x00")]
    found = client.get_many(probe)
    assert set(found) <= set(probe)
    for boundary in boundaries[:5]:
        before = list(client.scan_keys(end=boundary, reverse=True, limit=3))
        assert all(k < boundary for k in before)
        assert before == sorted(before, reverse=True)


async def _regions() -> list[Region]:
    async with tikv.AsyncRawClient(ENDPOINTS.split(",")) as client:
        return await client._pd.scan_regions(b"", b"", 0, Backoff(10))


async def test_async_client(prefix: bytes) -> None:
    ns = prefix + b"async/"
    async with tikv.AsyncRawClient(ENDPOINTS.split(",")) as client:
        keys = [ns + f"{i:04}".encode() for i in range(500)]
        await asyncio.gather(*(client.put(k, k) for k in keys))
        assert await asyncio.gather(*(client.get(k) for k in keys)) == keys
        assert [k async for k in client.scan_keys(prefix=ns)] == keys
        await client.delete_range(prefix=ns)
        assert await client.get_many(keys) == {}


def test_idle_connection_survives(client: tikv.RawClient, ns: bytes) -> None:
    """Keepalive pings on an idle channel must not get us disconnected."""
    client.put(ns + b"k", b"v")
    time.sleep(float(os.environ.get("TIKV_IDLE_SECONDS", "35")))
    assert client.get(ns + b"k") == b"v"


def test_unknown_endpoint_times_out() -> None:
    with tikv.RawClient("127.0.0.1:1", timeout=1) as client, pytest.raises(tikv.TimeoutError):
        client.get(b"k")
