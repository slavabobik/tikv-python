"""Argument validation and key-range arithmetic shared by both clients.

Internally an empty ``end`` key means "+inf", as on the wire.  The public API
uses ``None`` for that instead, because ``b""`` is itself a valid key.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterator, Sequence
from datetime import timedelta
from typing import TypeVar

T = TypeVar("T")


def as_bytes(value: object, what: str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):
        raise TypeError(f"{what} must be bytes, not str; encode it first, e.g. {what}.encode()")
    raise TypeError(f"{what} must be bytes, not {type(value).__name__}")


def as_key(key: object) -> bytes:
    return as_bytes(key, "key")


def as_value(value: object) -> bytes:
    return as_bytes(value, "value")


def ttl_seconds(ttl: float | timedelta | None) -> int:
    """Convert a TTL to whole seconds, rounding up; 0 means "no expiry"."""
    if ttl is None:
        return 0
    if isinstance(ttl, timedelta):
        seconds = ttl.total_seconds()
    elif isinstance(ttl, (int, float)) and not isinstance(ttl, bool):
        seconds = float(ttl)
    else:
        raise TypeError(f"ttl must be a number of seconds or a timedelta, not {type(ttl).__name__}")
    if not seconds > 0 or math.isinf(seconds):
        raise ValueError(f"ttl must be a positive, finite duration, got {ttl!r}")
    return math.ceil(seconds)


def prefix_end(prefix: bytes) -> bytes:
    """The smallest key greater than every key starting with *prefix*.

    Returns ``b""`` (+inf) when no such key exists, e.g. for ``b"\\xff\\xff"``.
    """
    stripped = prefix.rstrip(b"\xff")
    if not stripped:
        return b""
    return stripped[:-1] + bytes([stripped[-1] + 1])


def key_range(start: object | None, end: object | None, prefix: object | None) -> tuple[bytes, bytes] | None:
    """Resolve public ``start``/``end``/``prefix`` arguments to ``(start, end)``.

    Returns ``None`` when the range is empty.
    """
    if prefix is not None:
        if start is not None or end is not None:
            raise ValueError("pass either prefix or start/end, not both")
        lo = as_bytes(prefix, "prefix")
        return lo, prefix_end(lo)
    lo = b"" if start is None else as_bytes(start, "start")
    if end is None:
        return lo, b""
    hi = as_bytes(end, "end")
    return (lo, hi) if lo < hi else None


def min_end(a: bytes, b: bytes) -> bytes:
    """The smaller of two exclusive upper bounds, where ``b""`` is +inf."""
    if not a:
        return b
    if not b:
        return a
    return min(a, b)


def chunked(
    items: Sequence[T],
    *,
    max_count: int,
    max_bytes: int = 0,
    size: Callable[[T], int] | None = None,
) -> Iterator[list[T]]:
    """Split *items* into runs of at most *max_count* items.

    With a *size* function, runs also stay within *max_bytes*; a single item
    larger than that still gets a run of its own.
    """
    chunk: list[T] = []
    total = 0
    for item in items:
        item_size = size(item) if size is not None else 0
        if chunk and (len(chunk) >= max_count or (size is not None and total + item_size > max_bytes)):
            yield chunk
            chunk, total = [], 0
        chunk.append(item)
        total += item_size
    if chunk:
        yield chunk
