"""Concurrency primitives for the blocking flavor of the client.

Mirrors ``_async/_compat.py``.  Unlike the rest of ``_sync``, this module is
written by hand.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar

import grpc

T = TypeVar("T")
R = TypeVar("R")

Lock = threading.Lock
sleep = time.sleep
Channel = grpc.Channel

# Upper bound on requests in flight for one multi-region operation.
MAX_CONCURRENCY = 16


def map_concurrently(fn: Callable[[T], R], items: Sequence[T]) -> list[R]:
    """Run ``fn`` on every item concurrently and return results in order."""
    if len(items) <= 1:
        return [fn(item) for item in items]
    with ThreadPoolExecutor(max_workers=min(len(items), MAX_CONCURRENCY)) as pool:
        return list(pool.map(fn, items))


def open_channel(
    target: str,
    credentials: grpc.ChannelCredentials | None,
    options: Sequence[tuple[str, object]],
) -> grpc.Channel:
    if credentials is None:
        return grpc.insecure_channel(target, options=options)
    return grpc.secure_channel(target, credentials, options=options)
