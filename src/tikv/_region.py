"""Region routing: which region owns a key, and who leads it.

Everything here is pure, in-memory bookkeeping shared by the sync and async
clients.  Network I/O lives in ``_async`` (and the generated ``_sync``).
"""

from __future__ import annotations

import bisect
import threading
from dataclasses import dataclass, replace

from ._backoff import Wait
from ._errors import RegionError, UndeterminedError
from ._proto import errorpb_pb2, kvrpcpb_pb2, metapb_pb2

# Reverse scans need an exclusive upper bound, and TiKV treats an empty one as
# "nothing" rather than "+inf".  This key sorts after every key TiKV accepts
# (``storage.max-key-size`` defaults to 8 KiB), so it stands in for +inf.
MAX_KEY = b"\xff" * (8 * 1024 + 1)


@dataclass(frozen=True, slots=True)
class Region:
    """A contiguous key range ``[start, end)`` served by one Raft group.

    An empty ``end`` means the region extends to +inf.
    """

    meta: metapb_pb2.Region
    leader: metapb_pb2.Peer | None

    @property
    def id(self) -> int:
        return self.meta.id

    @property
    def start(self) -> bytes:
        return self.meta.start_key

    @property
    def end(self) -> bytes:
        return self.meta.end_key

    def contains(self, key: bytes) -> bool:
        return self.start <= key and (not self.end or key < self.end)

    def contains_end(self, key: bytes) -> bool:
        """Whether *key*, as an exclusive upper bound, falls in this region."""
        return self.start < key and (not self.end or key <= self.end)

    def context(self) -> kvrpcpb_pb2.Context:
        """The request context that routes a request to this region's leader."""
        return kvrpcpb_pb2.Context(
            region_id=self.meta.id, region_epoch=self.meta.region_epoch, peer=self.leader
        )

    def __repr__(self) -> str:
        epoch = self.meta.region_epoch
        leader = self.leader.store_id if self.leader else None
        return (
            f"Region(id={self.id}, start={self.start!r}, end={self.end!r}, "
            f"epoch={epoch.conf_ver}:{epoch.version}, leader_store={leader})"
        )


def make_region(meta: metapb_pb2.Region, leader: metapb_pb2.Peer | None) -> Region:
    """Build a :class:`Region`, treating PD's empty leader (id 0) as unknown."""
    return Region(meta, leader if leader is not None and leader.id else None)


class RegionCache:
    """Thread-safe cache of regions (sorted by start key) and store addresses."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._starts: list[bytes] = []
        self._regions: list[Region] = []
        self._stores: dict[int, str] = {}

    def find(self, key: bytes) -> Region | None:
        """The cached region containing *key*, if any."""
        with self._lock:
            i = bisect.bisect_right(self._starts, key) - 1
            if i >= 0 and self._regions[i].contains(key):
                return self._regions[i]
        return None

    def find_end(self, key: bytes) -> Region | None:
        """The cached region where *key* is a valid exclusive upper bound."""
        with self._lock:
            i = bisect.bisect_left(self._starts, key) - 1
            if i >= 0 and self._regions[i].contains_end(key):
                return self._regions[i]
        return None

    def put(self, region: Region) -> None:
        """Cache *region*, evicting any cached region it overlaps."""
        with self._lock:
            lo = bisect.bisect_right(self._starts, region.start) - 1
            if lo < 0 or not self._overlaps(self._regions[lo], region):
                lo += 1
            hi = lo
            while hi < len(self._regions) and self._overlaps(self._regions[hi], region):
                hi += 1
            self._starts[lo:hi] = [region.start]
            self._regions[lo:hi] = [region]

    @staticmethod
    def _overlaps(a: Region, b: Region) -> bool:
        return (not a.end or b.start < a.end) and (not b.end or a.start < b.end)

    def invalidate(self, region: Region) -> None:
        """Forget *region* so the next lookup asks PD again."""
        with self._lock:
            i = bisect.bisect_left(self._starts, region.start)
            if i < len(self._regions) and self._regions[i].id == region.id:
                del self._starts[i]
                del self._regions[i]

    def update_leader(self, region: Region, leader: metapb_pb2.Peer) -> bool:
        """Point *region* at a new *leader*; False if the peer is unknown."""
        for peer in region.meta.peers:
            if peer.store_id == leader.store_id:
                with self._lock:
                    i = bisect.bisect_left(self._starts, region.start)
                    # Only patch the entry we routed with; if it was replaced
                    # meanwhile, the newer entry wins.
                    if i < len(self._regions) and self._regions[i].meta == region.meta:
                        self._regions[i] = replace(region, leader=peer)
                return True
        self.invalidate(region)
        return False

    def store_address(self, store_id: int) -> str | None:
        return self._stores.get(store_id)

    def set_store_address(self, store_id: int, address: str) -> None:
        self._stores[store_id] = address

    def forget_store(self, store_id: int) -> None:
        self._stores.pop(store_id, None)

    def clear(self) -> None:
        with self._lock:
            self._starts.clear()
            self._regions.clear()
            self._stores.clear()


def on_region_error(
    cache: RegionCache,
    region: Region,
    error: errorpb_pb2.Error,
    *,
    idempotent: bool,
) -> tuple[Wait, float]:
    """Update *cache* after *region* reported *error*; decide how to retry.

    Returns the backoff reason and a server-suggested minimum delay.  Raises
    when retrying cannot help.  The policy follows client-go and client-rust.
    """
    store_id = region.leader.store_id if region.leader else 0
    kind = _kind(error)

    if kind == "not_leader":
        if error.not_leader.HasField("leader") and cache.update_leader(region, error.not_leader.leader):
            return Wait.REGION_SCHEDULING, 0.0
        cache.invalidate(region)  # leader unknown: an election is in progress
        return Wait.REGION_SCHEDULING, 0.0
    if kind == "server_is_busy":
        return Wait.SERVER_BUSY, error.server_is_busy.backoff_ms / 1000
    if kind == "raft_entry_too_large":
        raise RegionError(f"request too large for region {region.id}: {error.message}")
    if kind in _MAYBE_APPLIED and not idempotent:
        # The write may have been proposed before this error: replaying it
        # could apply it twice, so let the caller decide.
        detail = error.undetermined_result.message
        raise UndeterminedError(describe_region_error(region, error) + (f" ({detail})" if detail else ""))
    if kind in _SCHEDULING:
        return Wait.REGION_SCHEDULING, 0.0
    if kind == "disk_full":
        return Wait.SERVER_BUSY, 0.0

    # Epoch changes, missing regions, store mismatches and anything unknown:
    # the cached routing is stale, so refetch it from PD.
    cache.invalidate(region)
    if kind not in _REGION_ONLY and store_id:
        cache.forget_store(store_id)
    return Wait.REGION_MISS, 0.0


# Transient conditions that resolve once the region settles.
_SCHEDULING = frozenset(
    {
        "stale_command",
        "max_timestamp_not_synced",
        "read_index_not_ready",
        "proposal_in_merging_mode",
        "data_is_not_ready",
        "region_not_initialized",
        "RecoveryInProgress",
        "FlashbackInProgress",
        "FlashbackNotPrepared",
        "undetermined_result",
    }
)
# Errors a store may report after proposing a write to Raft, so the write
# might still be applied: TiKV answers pending proposals with RegionNotFound
# when the peer is destroyed, and with StaleCommand when leadership changes.
_MAYBE_APPLIED = frozenset({"undetermined_result", "region_not_found", "stale_command"})
# Stale routing that does not implicate the store address.
_REGION_ONLY = frozenset({"epoch_not_match", "region_not_found", "key_not_in_region"})

_KINDS = tuple(field.name for field in errorpb_pb2.Error.DESCRIPTOR.fields if field.message_type is not None)


def describe_region_error(region: Region, error: errorpb_pb2.Error) -> str:
    """A one-line summary such as ``region 12: epoch_not_match: ...``."""
    return f"region {region.id}: {_kind(error) or 'error'}: {error.message}".rstrip(": ")


def _kind(error: errorpb_pb2.Error) -> str | None:
    """The name of the specific error field set on *error*, if any."""
    for name in _KINDS:
        if error.HasField(name):
            return name
    return None
