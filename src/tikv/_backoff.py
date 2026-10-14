"""Exponential backoff bounded by a per-operation deadline."""

from __future__ import annotations

import enum
import logging
import random
import time

from ._errors import TimeoutError

log = logging.getLogger("tikv")


class Wait(enum.Enum):
    """Why an operation is backing off; each reason has its own schedule.

    Values are ``(base, cap)`` in seconds, following client-go's defaults.
    """

    REGION_MISS = (0.002, 0.5)  # stale region cache: refresh and retry soon
    REGION_SCHEDULING = (0.002, 0.5)  # leader election, merge, split in progress
    TIKV_RPC = (0.1, 2.0)  # store unreachable
    PD_RPC = (0.1, 3.0)  # placement driver unreachable or electing a leader
    SERVER_BUSY = (2.0, 10.0)  # store is overloaded


class Backoff:
    """Tracks retries of one logical operation against its deadline."""

    __slots__ = ("_attempts", "deadline", "timeout")

    def __init__(self, timeout: float, *, deadline: float | None = None) -> None:
        self.timeout = timeout
        self.deadline = time.monotonic() + timeout if deadline is None else deadline
        self._attempts: dict[Wait, int] = {}

    def remaining(self) -> float:
        """Seconds left before the deadline, never negative."""
        return max(0.0, self.deadline - time.monotonic())

    def fork(self) -> Backoff:
        """A backoff for a sub-request: same deadline and retry history.

        The fork escalates independently from here on, so concurrent
        sub-requests do not inflate each other's delays.
        """
        forked = Backoff(self.timeout, deadline=self.deadline)
        forked._attempts = dict(self._attempts)
        return forked

    def delay(self, wait: Wait, cause: BaseException, *, hint: float = 0.0) -> float:
        """Return how long to sleep before the next attempt.

        Raises :class:`~tikv.TimeoutError` from *cause* when sleeping would
        overrun the deadline, so callers never retry past their budget.
        """
        base, cap = wait.value
        attempt = self._attempts.get(wait, 0)
        self._attempts[wait] = attempt + 1
        ceiling = min(cap, base * 2.0**attempt)
        # "Equal jitter": at least half the ceiling, so retries stay spread out.
        delay = max(hint, ceiling / 2 + random.uniform(0, ceiling / 2))
        if delay >= self.remaining():
            raise TimeoutError(
                f"operation did not complete within {self.timeout:g}s: {describe(cause)}"
            ) from cause
        log.debug("retrying in %.3fs (%s): %s", delay, wait.name.lower(), describe(cause))
        return delay


def describe(error: BaseException) -> str:
    """A one-line description of *error*, including gRPC status details."""
    details = getattr(error, "details", None)
    code = getattr(error, "code", None)
    if callable(details) and callable(code):
        return f"gRPC {code().name}: {details()}"
    return str(error) or type(error).__name__
