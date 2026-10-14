"""Client for the Placement Driver (PD), TiKV's cluster metadata service."""

from __future__ import annotations

from collections.abc import Iterable

import grpc
from google.protobuf.message import Message

from .._backoff import Backoff, Wait, describe, log
from .._errors import PDError
from .._proto import pdpb_pb2 as pdpb
from .._region import Region, RegionCache, make_region
from .._rpc import (
    PD_GET_MEMBERS,
    PD_GET_PREV_REGION,
    PD_GET_REGION,
    PD_GET_STORE,
    PD_SCAN_REGIONS,
    Method,
    Response,
    is_retryable,
    strip_scheme,
)
from ._compat import Lock, sleep
from ._transport import Transport

CALLER_ID = "tikv-python"

# PD answers metadata queries quickly; a slower reply means a sick leader,
# so give up on it early and rediscover instead of burning the whole budget.
RPC_TIMEOUT = 3.0


class PDClient:
    """Finds the PD leader and asks it where regions and stores are."""

    def __init__(self, endpoints: Iterable[str], transport: Transport, cache: RegionCache) -> None:
        self._endpoints = list(dict.fromkeys(map(strip_scheme, endpoints)))
        self._transport = transport
        self._cache = cache
        self._lock = Lock()
        self._leader: str | None = None
        self.cluster_id = 0

    async def region(self, key: bytes, backoff: Backoff) -> Region:
        """The region containing *key*."""
        while True:
            region = await self._fetch_region(PD_GET_REGION, key, backoff)
            if region is not None:
                return region
            # PD has no region for this key yet, e.g. right after bootstrap.
            await sleep(backoff.delay(Wait.REGION_MISS, PDError(f"no region found for key {key!r}")))

    async def region_by_end(self, key: bytes, backoff: Backoff) -> Region:
        """The region in which *key* is a valid exclusive upper bound."""
        while True:
            region = await self.region(key, backoff)
            if region.start != key or not key:
                return region
            # *key* opens this region, so the keys below it live in the one
            # before.  GetPrevRegion returns exactly that (like client-go).
            previous = await self._fetch_region(PD_GET_PREV_REGION, key, backoff)
            if previous is not None:
                return previous
            # The regions merged in between: start over.
            await sleep(backoff.delay(Wait.REGION_MISS, PDError(f"no region found before key {key!r}")))

    async def _fetch_region(
        self, method: Method[pdpb.GetRegionResponse], key: bytes, backoff: Backoff
    ) -> Region | None:
        response = await self._call(method, pdpb.GetRegionRequest(region_key=key), backoff)
        if not response.HasField("region"):
            return None
        region = make_region(response.region, response.leader)
        self._cache.put(region)
        return region

    async def scan_regions(self, start: bytes, end: bytes, limit: int, backoff: Backoff) -> list[Region]:
        """Up to *limit* consecutive regions overlapping ``[start, end)``."""
        request = pdpb.ScanRegionsRequest(start_key=start, end_key=end, limit=limit)
        response = await self._call(PD_SCAN_REGIONS, request, backoff)
        if response.regions:
            regions = [make_region(r.region, r.leader) for r in response.regions]
        else:  # older PD versions only fill the legacy fields
            regions = [
                make_region(m, leader)
                for m, leader in zip(response.region_metas, response.leaders, strict=False)
            ]
        for region in regions:
            self._cache.put(region)
        return regions

    async def store_address(self, store_id: int, backoff: Backoff) -> str:
        """The client-facing address of TiKV store *store_id*."""
        address = self._cache.store_address(store_id)
        if address is None:
            response = await self._call(PD_GET_STORE, pdpb.GetStoreRequest(store_id=store_id), backoff)
            address = strip_scheme(response.store.address)
            self._cache.set_store_address(store_id, address)
        return address

    async def _call(self, method: Method[Response], request: Message, backoff: Backoff) -> Response:
        """Send *request* to the PD leader, following leader changes."""
        while True:
            leader = await self._leader_address(backoff)
            request.header.cluster_id = self.cluster_id  # type: ignore[attr-defined]
            request.header.caller_id = CALLER_ID  # type: ignore[attr-defined]
            timeout = min(RPC_TIMEOUT, backoff.remaining())
            try:
                response = await self._transport.call(leader, method, request, timeout)
            except grpc.RpcError as error:
                if not is_retryable(error):
                    raise PDError(f"{method.path} failed: {describe(error)}") from error
                self._leader = None  # the leader may have moved; rediscover it
                await sleep(backoff.delay(Wait.PD_RPC, error))
                continue

            header_error = response.header.error  # type: ignore[attr-defined]
            if header_error.type == pdpb.OK:
                return response
            failure = PDError(
                f"{method.path} failed: {pdpb.ErrorType.Name(header_error.type)}: {header_error.message}"
            )
            if header_error.type != pdpb.REGION_NOT_FOUND:
                raise failure
            await sleep(backoff.delay(Wait.REGION_MISS, failure))

    async def _leader_address(self, backoff: Backoff) -> str:
        leader = self._leader
        if leader is not None:
            return leader
        async with self._lock:
            while self._leader is None:
                await self._discover(backoff)
            return self._leader

    async def _discover(self, backoff: Backoff) -> None:
        """Ask the known PD members who the leader is."""
        cause: Exception = PDError("no PD endpoint answered")
        rejected: list[str] = []
        for endpoint in list(self._endpoints):
            timeout = min(RPC_TIMEOUT, backoff.remaining())
            try:
                response = await self._transport.call(
                    endpoint, PD_GET_MEMBERS, pdpb.GetMembersRequest(), timeout
                )
            except grpc.RpcError as error:
                if not is_retryable(error):
                    rejected.append(f"{endpoint} is not a PD server: {describe(error)}")
                cause = error
                continue
            if not response.leader.client_urls:
                cause = PDError(f"PD at {endpoint} reports no leader (election in progress?)")
                continue
            # Learn about every member so we survive losing the seed endpoints.
            for member in response.members:
                for url in member.client_urls:
                    if strip_scheme(url) not in self._endpoints:
                        self._endpoints.append(strip_scheme(url))
            self.cluster_id = response.header.cluster_id
            self._leader = strip_scheme(response.leader.client_urls[0])
            log.debug("PD leader is %s (cluster %d)", self._leader, self.cluster_id)
            return
        if len(rejected) == len(self._endpoints):  # retrying cannot help
            raise PDError("; ".join(rejected)) from cause
        await sleep(backoff.delay(Wait.PD_RPC, cause))
