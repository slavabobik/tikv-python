"""Exceptions raised by the TiKV client.

Every exception derives from :class:`TiKVError`, so ``except tikv.TiKVError``
catches anything the library raises on purpose.  Transient failures (leader
changes, region splits, busy servers, unreachable stores) are retried
internally and only surface as :class:`TimeoutError` once the operation's time
budget runs out.
"""

from __future__ import annotations

import builtins

__all__ = [
    "PDError",
    "RegionError",
    "ServerError",
    "TiKVError",
    "TimeoutError",
    "UndeterminedError",
]


class TiKVError(Exception):
    """Base class for all errors raised by this library."""


class TimeoutError(TiKVError, builtins.TimeoutError):
    """The operation did not complete within the client's ``timeout``.

    The last transient error, if any, is attached as ``__cause__``.
    """


class PDError(TiKVError):
    """The Placement Driver rejected a request."""


class ServerError(TiKVError):
    """A TiKV server rejected a request, for example because a key is too large."""


class RegionError(TiKVError):
    """A region error that retrying cannot resolve, such as an oversized write."""


class UndeterminedError(TiKVError):
    """A non-idempotent request failed in flight; it may or may not have been applied.

    Raised only by :meth:`RawClient.compare_and_swap`, which is never retried
    blindly: replaying a swap that already succeeded would report a spurious
    failure.  Read the key to find out what happened.
    """
