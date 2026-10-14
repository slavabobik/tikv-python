"""Concurrency primitives for the asyncio flavor of the client.

``_sync/_compat.py`` provides the same names for the blocking flavor; the rest
of ``_sync`` is generated from ``_async`` by ``scripts/unasync.py``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar

import grpc
import grpc.aio

T = TypeVar("T")
R = TypeVar("R")

Lock = asyncio.Lock
sleep = asyncio.sleep
Channel = grpc.aio.Channel

# Upper bound on requests in flight for one multi-region operation.
MAX_CONCURRENCY = 16


async def map_concurrently(fn: Callable[[T], Awaitable[R]], items: Sequence[T]) -> list[R]:
    """Run ``fn`` on every item concurrently and return results in order."""
    if len(items) <= 1:
        return [await fn(item) for item in items]
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    async def bounded(item: T) -> R:
        async with semaphore:
            return await fn(item)

    tasks = [asyncio.ensure_future(bounded(item)) for item in items]
    try:
        return list(await asyncio.gather(*tasks))
    except BaseException:
        for task in tasks:
            task.cancel()
        raise


def open_channel(
    target: str,
    credentials: grpc.ChannelCredentials | None,
    options: Sequence[tuple[str, object]],
) -> grpc.aio.Channel:
    if credentials is None:
        return grpc.aio.insecure_channel(target, options=options)
    return grpc.aio.secure_channel(target, credentials, options=options)
