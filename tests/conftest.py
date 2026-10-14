from __future__ import annotations

from collections.abc import AsyncIterator, Iterator

import pytest

import tikv

from .fake_cluster import FakeCluster


@pytest.fixture
def cluster() -> Iterator[FakeCluster]:
    fake = FakeCluster()
    yield fake
    fake.stop()


@pytest.fixture
def client(cluster: FakeCluster) -> Iterator[tikv.RawClient]:
    with tikv.RawClient(cluster.address, timeout=5) as client:
        yield client


@pytest.fixture
def atomic_client(cluster: FakeCluster) -> Iterator[tikv.RawClient]:
    with tikv.RawClient(cluster.address, timeout=5, atomic=True) as client:
        yield client


@pytest.fixture
async def aclient(cluster: FakeCluster) -> AsyncIterator[tikv.AsyncRawClient]:
    async with tikv.AsyncRawClient(cluster.address, timeout=5) as client:
        yield client
