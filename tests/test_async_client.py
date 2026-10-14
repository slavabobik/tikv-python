"""The asyncio surface of the client."""

from __future__ import annotations

import asyncio

import pytest

import tikv

from .fake_cluster import FakeCluster


async def test_round_trip(aclient: tikv.AsyncRawClient) -> None:
    await aclient.put(b"k", b"v", ttl=60)
    assert await aclient.get(b"k") == b"v"
    assert await aclient.ttl(b"k") == 60
    await aclient.delete(b"k")
    assert await aclient.get(b"k") is None


async def test_batches_and_scans(aclient: tikv.AsyncRawClient, cluster: FakeCluster) -> None:
    cluster.split(b"k3", b"k6")
    items = {f"k{i}".encode(): str(i).encode() for i in range(10)}
    await aclient.put_many(items)
    assert await aclient.get_many(items) == items
    assert [k async for k, _ in aclient.scan(b"k2", b"k5")] == [b"k2", b"k3", b"k4"]
    assert [k async for k in aclient.scan_keys(prefix=b"k", reverse=True, limit=2)] == [b"k9", b"k8"]
    await aclient.delete_many([b"k0", b"k1"])
    await aclient.delete_range(b"k5")
    assert [k async for k in aclient.scan_keys()] == [b"k2", b"k3", b"k4"]


async def test_concurrent_requests_share_one_client(
    aclient: tikv.AsyncRawClient, cluster: FakeCluster
) -> None:
    cluster.split(*(f"{i:03}".encode() for i in range(0, 200, 20)))
    keys = [f"{i:03}".encode() for i in range(200)]
    await asyncio.gather(*(aclient.put(k, k) for k in keys))
    assert await asyncio.gather(*(aclient.get(k) for k in keys)) == keys


async def test_compare_and_swap(cluster: FakeCluster) -> None:
    async with tikv.AsyncRawClient(cluster.address, atomic=True) as client:
        assert await client.compare_and_swap(b"k", None, b"1") == (True, None)
        assert await client.compare_and_swap(b"k", b"1", b"2") == (True, b"1")


async def test_timeout(cluster: FakeCluster) -> None:
    async with tikv.AsyncRawClient("127.0.0.1:1", timeout=0.3) as client:
        with pytest.raises(tikv.TimeoutError):
            await client.get(b"k")


async def test_cancellation_propagates(aclient: tikv.AsyncRawClient) -> None:
    task = asyncio.ensure_future(aclient.put_many({bytes([i]): b"" for i in range(256)}))
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await aclient.get(b"\x00") in (None, b"")  # the client is still usable
