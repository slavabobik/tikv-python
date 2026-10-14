"""The raw key-value client."""

from __future__ import annotations

import math
from collections.abc import AsyncIterator, Callable, Iterable, Mapping, Sequence
from datetime import timedelta
from types import TracebackType
from typing import TypeVar

import grpc
from google.protobuf.message import Message

from .._backoff import Backoff, Wait, describe
from .._errors import RegionError, ServerError, TiKVError, UndeterminedError
from .._keys import as_key, as_value, chunked, key_range, min_end, ttl_seconds
from .._proto import kvrpcpb_pb2 as kvrpcpb
from .._region import MAX_KEY, Region, RegionCache, describe_region_error, on_region_error
from .._rpc import (
    RAW_BATCH_DELETE,
    RAW_BATCH_GET,
    RAW_BATCH_PUT,
    RAW_CHECKSUM,
    RAW_COMPARE_AND_SWAP,
    RAW_DELETE,
    RAW_DELETE_RANGE,
    RAW_GET,
    RAW_GET_KEY_TTL,
    RAW_PUT,
    RAW_SCAN,
    Method,
    Response,
    TLSConfig,
    is_retryable,
)
from .._types import CasResult, Checksum
from ._compat import map_concurrently, sleep
from ._pd import PDClient
from ._transport import Transport

T = TypeVar("T")

# Per-request limits when splitting batches, following client-go.
BATCH_MAX_KEYS = 512
BATCH_MAX_BYTES = 1024 * 1024
# Keys fetched per RawScan request when the caller does not set a limit.
SCAN_PAGE_SIZE = 512
# Regions fetched from PD in one go when walking a key range.
REGION_PREFETCH = 64


class AsyncRawClient:
    """A client for TiKV's raw (non-transactional) key-value API.

    Keys and values are :class:`bytes`.  Each single-key operation is atomic;
    batch operations are atomic per region but not across regions.

    ::

        async with AsyncRawClient(["127.0.0.1:2379"]) as client:
            await client.put(b"greeting", b"hello")
            assert await client.get(b"greeting") == b"hello"

    :param pd_endpoints: Address of one or more PD servers, e.g.
        ``"pd-0:2379"``.  The rest of the cluster is discovered from them.
    :param timeout: Time budget in seconds for each operation, including all
        retries.  Scans apply it to each page they fetch.
    :param atomic: Route every write through TiKV's single-row transaction
        path.  Required by :meth:`compare_and_swap`; every client writing the
        same keys must agree on this setting.
    :param tls: Certificates for mutual TLS, if the cluster requires it.

    One client can serve a whole application: it is safe to use from many
    threads (blocking flavor) or tasks (asyncio flavor) at once, and it keeps
    one connection per server.  Close it with :meth:`close` or use it as a
    context manager.
    """

    def __init__(
        self,
        pd_endpoints: str | Iterable[str],
        *,
        timeout: float = 10.0,
        atomic: bool = False,
        tls: TLSConfig | None = None,
    ) -> None:
        endpoints = [pd_endpoints] if isinstance(pd_endpoints, str) else list(pd_endpoints)
        if not endpoints:
            raise ValueError("at least one PD endpoint is required")
        if not timeout > 0:
            raise ValueError(f"timeout must be positive, got {timeout!r}")
        self.timeout = timeout
        self.atomic = atomic
        self._endpoints = endpoints
        self._cache = RegionCache()
        self._transport = Transport(tls.credentials() if tls else None)
        self._pd = PDClient(endpoints, self._transport, self._cache)
        self._closed = False

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._endpoints!r}, timeout={self.timeout!r}, atomic={self.atomic!r})"

    async def __aenter__(self) -> AsyncRawClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """Close all connections.  The client cannot be used afterwards."""
        if not self._closed:
            self._closed = True
            await self._transport.close()
            self._cache.clear()

    # -- Reads ---------------------------------------------------------------

    async def get(self, key: bytes) -> bytes | None:
        """Return the value of *key*, or ``None`` if it does not exist."""
        request = kvrpcpb.RawGetRequest(key=as_key(key))
        response = await self._execute(request.key, RAW_GET, request)
        return None if response.not_found else response.value

    async def get_many(self, keys: Iterable[bytes]) -> dict[bytes, bytes]:
        """Return a dict of the given keys that exist, mapped to their values.

        Missing keys are left out.  The dict follows the order of *keys*.
        """
        wanted = list(dict.fromkeys(map(as_key, keys)))
        responses = await self._execute_batch(
            sorted(wanted),
            key=_identity,
            method=RAW_BATCH_GET,
            build=lambda chunk: kvrpcpb.RawBatchGetRequest(keys=chunk),
        )
        found = {pair.key: pair.value for response in responses for pair in _checked(response.pairs)}
        return {key: found[key] for key in wanted if key in found}

    async def ttl(self, key: bytes) -> int | None:
        """Return the seconds until *key* expires, or ``None`` if it never does.

        :raises KeyError: if *key* does not exist.
        """
        request = kvrpcpb.RawGetKeyTTLRequest(key=as_key(key))
        response = await self._execute(request.key, RAW_GET_KEY_TTL, request)
        if response.not_found:
            raise KeyError(key)
        return response.ttl or None

    async def scan(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
        *,
        prefix: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        """Iterate over ``(key, value)`` pairs in key order.

        Covers keys in ``[start, end)``, or every key starting with *prefix*.
        ``None`` bounds are open-ended.  Results are fetched lazily, a page at
        a time, so scanning a large range uses little memory::

            async for key, value in client.scan(prefix=b"user:"):
                ...

        :param limit: Stop after this many pairs.
        :param reverse: Iterate from the largest key down.
        """
        async for pair in self._scan(start, end, prefix, limit, reverse, key_only=False):
            yield pair.key, pair.value

    async def scan_keys(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
        *,
        prefix: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
    ) -> AsyncIterator[bytes]:
        """Like :meth:`scan`, but yields only keys, without fetching values."""
        async for pair in self._scan(start, end, prefix, limit, reverse, key_only=True):
            yield pair.key

    async def checksum(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
        *,
        prefix: bytes | None = None,
    ) -> Checksum:
        """Fingerprint the keys in ``[start, end)``, or those starting with *prefix*.

        The servers compute it in place, so no data crosses the network.
        """
        bounds = key_range(start, end, prefix)
        if bounds is None:
            return Checksum(0, 0, 0)
        responses = await self._execute_range(
            *bounds,
            method=RAW_CHECKSUM,
            build=lambda lo, hi: kvrpcpb.RawChecksumRequest(
                ranges=[kvrpcpb.KeyRange(start_key=lo, end_key=hi)]
            ),
        )
        crc64_xor = keys = size = 0
        for response in responses:
            crc64_xor ^= response.checksum
            keys += response.total_kvs
            size += response.total_bytes
        return Checksum(crc64_xor, keys, size)

    # -- Writes --------------------------------------------------------------

    async def put(self, key: bytes, value: bytes, *, ttl: float | timedelta | None = None) -> None:
        """Set *key* to *value*.

        :param ttl: Expire the key after this many seconds (rounded up to a
            whole second) or this :class:`~datetime.timedelta`.  Requires
            ``storage.enable-ttl`` on the TiKV servers.
        """
        request = kvrpcpb.RawPutRequest(
            key=as_key(key), value=as_value(value), ttl=ttl_seconds(ttl), for_cas=self.atomic
        )
        await self._execute(request.key, RAW_PUT, request)

    async def put_many(
        self,
        items: Mapping[bytes, bytes] | Iterable[tuple[bytes, bytes]],
        *,
        ttl: float | timedelta | None = None,
    ) -> None:
        """Set many keys at once, from a mapping or ``(key, value)`` pairs.

        Writes are atomic per region but not across regions.

        :param ttl: Expiry applied to every key; see :meth:`put`.
        """
        entries = sorted((as_key(k), as_value(v)) for k, v in dict(items).items())
        ttls = [ttl_seconds(ttl)] if ttl is not None else []

        def build(chunk: list[tuple[bytes, bytes]]) -> kvrpcpb.RawBatchPutRequest:
            return kvrpcpb.RawBatchPutRequest(
                pairs=[kvrpcpb.KvPair(key=k, value=v) for k, v in chunk], ttls=ttls, for_cas=self.atomic
            )

        await self._execute_batch(entries, key=_first, method=RAW_BATCH_PUT, build=build, size=_pair_size)

    async def delete(self, key: bytes) -> None:
        """Delete *key*.  Deleting a missing key is not an error."""
        request = kvrpcpb.RawDeleteRequest(key=as_key(key), for_cas=self.atomic)
        await self._execute(request.key, RAW_DELETE, request)

    async def delete_many(self, keys: Iterable[bytes]) -> None:
        """Delete many keys at once.  Missing keys are ignored."""
        await self._execute_batch(
            sorted(set(map(as_key, keys))),
            key=_identity,
            method=RAW_BATCH_DELETE,
            build=lambda chunk: kvrpcpb.RawBatchDeleteRequest(keys=chunk, for_cas=self.atomic),
        )

    async def delete_range(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
        *,
        prefix: bytes | None = None,
    ) -> None:
        """Delete every key in ``[start, end)``, or every key starting with *prefix*.

        ``end=None`` deletes through the end of the key space.  To guard
        against accidents, *start* or a non-empty *prefix* is required:
        wiping everything takes an explicit ``delete_range(b"")``.
        """
        if self.atomic:
            raise TiKVError("delete_range is not supported by clients in atomic mode")
        if prefix is None and start is None:
            raise TypeError("delete_range() requires start or prefix")
        if prefix is not None and not prefix:
            raise ValueError('an empty prefix matches every key; use delete_range(b"") to wipe them all')
        bounds = key_range(start, end, prefix)
        if bounds is not None:
            await self._execute_range(
                *bounds,
                method=RAW_DELETE_RANGE,
                build=lambda lo, hi: kvrpcpb.RawDeleteRangeRequest(start_key=lo, end_key=hi),
            )

    async def compare_and_swap(
        self,
        key: bytes,
        expected: bytes | None,
        new: bytes,
        *,
        ttl: float | timedelta | None = None,
    ) -> CasResult:
        """Set *key* to *new* only if its current value is *expected*.

        Pass ``expected=None`` to require that *key* does not exist yet.
        Requires a client created with ``atomic=True``::

            swapped, previous = await client.compare_and_swap(b"lock", None, b"owner-1")

        :raises UndeterminedError: if the request failed in flight, so the
            swap may or may not have happened.
        """
        if not self.atomic:
            raise TiKVError("compare_and_swap requires a client created with atomic=True")
        request = kvrpcpb.RawCASRequest(
            key=as_key(key),
            value=as_value(new),
            previous_not_exist=expected is None,
            previous_value=b"" if expected is None else as_value(expected),
            ttl=ttl_seconds(ttl),
        )
        response = await self._execute(request.key, RAW_COMPARE_AND_SWAP, request, idempotent=False)
        return CasResult(response.succeed, None if response.previous_not_exist else response.previous_value)

    # -- Request routing -----------------------------------------------------

    async def _execute(
        self, key: bytes, method: Method[Response], request: Message, *, idempotent: bool = True
    ) -> Response:
        """Send a single-key request to the region owning *key*."""
        backoff = Backoff(self.timeout)
        while True:
            region = await self._locate(key, backoff)
            response = await self._send(region, method, request, backoff, idempotent=idempotent)
            if response is not None:
                return response

    async def _execute_batch(
        self,
        items: Sequence[T],
        *,
        key: Callable[[T], bytes],
        method: Method[Response],
        build: Callable[[list[T]], Message],
        size: Callable[[T], int] | None = None,
        backoff: Backoff | None = None,
    ) -> list[Response]:
        """Split key-sorted *items* by region and send one request per chunk."""
        backoff = backoff or Backoff(self.timeout)
        batches = await self._plan_batches(items, key, size, backoff)

        async def run(batch: tuple[Region, list[T]]) -> list[Response]:
            region, chunk = batch
            retry = backoff.fork()
            while True:
                response = await self._send(region, method, build(chunk), retry)
                if response is not None:
                    return [response]
                # The region changed under us: re-route this chunk.
                replanned = await self._plan_batches(chunk, key, size, retry)
                if len(replanned) > 1:  # it now spans several regions
                    return await self._execute_batch(
                        chunk, key=key, method=method, build=build, size=size, backoff=retry
                    )
                region, chunk = replanned[0]

        results = await map_concurrently(run, batches)
        return [response for group in results for response in group]

    async def _plan_batches(
        self, items: Sequence[T], key: Callable[[T], bytes], size: Callable[[T], int] | None, backoff: Backoff
    ) -> list[tuple[Region, list[T]]]:
        """Group key-sorted *items* by region, in chunks small enough for one request."""
        batches: list[tuple[Region, list[T]]] = []
        i = 0
        while i < len(items):
            region = await self._locate(key(items[i]), backoff, until=key(items[-1]) + b"\0")
            j = i + 1
            while j < len(items) and region.contains(key(items[j])):
                j += 1
            for chunk in chunked(items[i:j], max_count=BATCH_MAX_KEYS, max_bytes=BATCH_MAX_BYTES, size=size):
                batches.append((region, chunk))
            i = j
        await self._resolve_stores([region for region, _ in batches], backoff)
        return batches

    async def _execute_range(
        self,
        start: bytes,
        end: bytes,
        *,
        method: Method[Response],
        build: Callable[[bytes, bytes], Message],
        backoff: Backoff | None = None,
    ) -> list[Response]:
        """Send one request per region overlapping ``[start, end)``."""
        backoff = backoff or Backoff(self.timeout)
        spans = await self._plan_spans(start, end, backoff)

        async def run(span: tuple[Region, bytes, bytes]) -> list[Response]:
            region, lo, hi = span
            retry = backoff.fork()
            while True:
                response = await self._send(region, method, build(lo, hi), retry)
                if response is not None:
                    return [response]
                replanned = await self._plan_spans(lo, hi, retry)
                if len(replanned) > 1:
                    return await self._execute_range(lo, hi, method=method, build=build, backoff=retry)
                region, lo, hi = replanned[0]

        results = await map_concurrently(run, spans)
        return [response for group in results for response in group]

    async def _plan_spans(
        self, start: bytes, end: bytes, backoff: Backoff
    ) -> list[tuple[Region, bytes, bytes]]:
        """Split ``[start, end)`` at region boundaries."""
        spans: list[tuple[Region, bytes, bytes]] = []
        cursor = start
        while True:
            region = await self._locate(cursor, backoff, until=end)
            spans.append((region, cursor, min_end(end, region.end)))
            if not region.end or (end and region.end >= end):
                break
            cursor = region.end
        await self._resolve_stores([region for region, _, _ in spans], backoff)
        return spans

    async def _scan(
        self,
        start: bytes | None,
        end: bytes | None,
        prefix: bytes | None,
        limit: int | None,
        reverse: bool,
        *,
        key_only: bool,
    ) -> AsyncIterator[kvrpcpb.KvPair]:
        if limit is not None and limit < 0:
            raise ValueError(f"limit must not be negative, got {limit!r}")
        bounds = key_range(start, end, prefix)
        if bounds is None:
            return
        lower, upper = bounds
        remaining = math.inf if limit is None else limit
        # Forward scans move a lower bound up; reverse scans move an upper bound down.
        cursor = (upper or MAX_KEY) if reverse else lower

        while remaining > 0:
            page = int(min(remaining, SCAN_PAGE_SIZE))
            backoff = Backoff(self.timeout)
            while True:
                if reverse:
                    region = await self._locate_end(cursor, backoff)
                    request = kvrpcpb.RawScanRequest(
                        start_key=cursor, end_key=max(lower, region.start), reverse=True
                    )
                else:
                    region = await self._locate(cursor, backoff)
                    request = kvrpcpb.RawScanRequest(start_key=cursor, end_key=min_end(upper, region.end))
                request.limit = page
                request.key_only = key_only
                response = await self._send(region, RAW_SCAN, request, backoff)
                if response is not None:
                    break

            for pair in _checked(response.kvs):
                yield pair
            remaining -= len(response.kvs)

            if len(response.kvs) == page:  # the region may hold more
                last = response.kvs[-1].key
                if reverse:
                    if last <= lower:
                        return
                    cursor = last
                else:
                    cursor = last + b"\0"
                    if upper and cursor >= upper:
                        return
            elif reverse:
                if region.start <= lower:
                    return
                cursor = region.start
            else:
                if not region.end or (upper and region.end >= upper):
                    return
                cursor = region.end

    async def _locate(self, key: bytes, backoff: Backoff, *, until: bytes | None = None) -> Region:
        """The region containing *key*.

        On a cache miss, *until* lets PD return every region up to that key at
        once, which saves round trips when walking a range or a sorted batch.
        """
        region = self._cache.find(key)
        if region is not None:
            return region
        if until is None or (until and until <= key):
            return await self._pd.region(key, backoff)
        regions = await self._pd.scan_regions(key, until, REGION_PREFETCH, backoff)
        if regions and regions[0].contains(key):
            return regions[0]
        return await self._pd.region(key, backoff)

    async def _resolve_stores(self, regions: Iterable[Region], backoff: Backoff) -> None:
        """Look up leader store addresses once, before fanning out requests."""
        for store_id in sorted({r.leader.store_id for r in regions if r.leader is not None}):
            await self._pd.store_address(store_id, backoff)

    async def _locate_end(self, key: bytes, backoff: Backoff) -> Region:
        """The region in which *key* is a valid exclusive upper bound."""
        region = self._cache.find_end(key)
        if region is not None:
            return region
        return await self._pd.region_by_end(key, backoff)

    async def _send(
        self,
        region: Region,
        method: Method[Response],
        request: Message,
        backoff: Backoff,
        *,
        idempotent: bool = True,
    ) -> Response | None:
        """Send *request* to the leader of *region*.

        Returns ``None`` when the request must be re-routed, after updating
        the region cache and backing off as needed.
        """
        if self._closed:
            raise TiKVError("the client is closed")
        if region.leader is None:  # PD knows no leader: an election is under way
            self._cache.invalidate(region)
            await sleep(backoff.delay(Wait.REGION_SCHEDULING, RegionError(f"{region} has no leader")))
            return None

        store_id = region.leader.store_id
        address = await self._pd.store_address(store_id, backoff)
        request.context.CopyFrom(region.context())  # type: ignore[attr-defined]
        try:
            response = await self._transport.call(address, method, request, backoff.remaining())
        except grpc.RpcError as error:
            if not is_retryable(error):
                raise ServerError(f"{method.path} failed: {describe(error)}") from error
            if not idempotent:
                raise UndeterminedError(f"{method.path} to {address} failed: {describe(error)}") from error
            self._cache.invalidate(region)
            self._cache.forget_store(store_id)
            await sleep(backoff.delay(Wait.TIKV_RPC, error))
            return None

        if response.HasField("region_error"):
            region_error = response.region_error  # type: ignore[attr-defined]
            wait, hint = on_region_error(self._cache, region, region_error, idempotent=idempotent)
            cause = RegionError(describe_region_error(region, region_error))
            await sleep(backoff.delay(wait, cause, hint=hint))
            return None
        if getattr(response, "error", ""):
            raise ServerError(response.error)  # type: ignore[attr-defined]
        return response


def _checked(pairs: Iterable[kvrpcpb.KvPair]) -> Iterable[kvrpcpb.KvPair]:
    for pair in pairs:
        if pair.HasField("error"):
            raise ServerError(pair.error.abort or pair.error.retryable or str(pair.error))
    return pairs


def _identity(key: bytes) -> bytes:
    return key


def _first(pair: tuple[bytes, bytes]) -> bytes:
    return pair[0]


def _pair_size(pair: tuple[bytes, bytes]) -> int:
    return len(pair[0]) + len(pair[1])
