"""An in-process PD + TiKV that speaks the real gRPC protocol.

It keeps data in memory and models what the client must cope with: regions
that split, leaders that move, epochs that go stale, and servers that fail.
Faults are injected per method with :meth:`FakeCluster.fail`.
"""

from __future__ import annotations

import bisect
import math
import threading
import time
import zlib
from collections import Counter, deque
from collections.abc import Callable
from concurrent import futures
from dataclasses import dataclass, field
from typing import Any

import grpc

from tikv._proto import errorpb_pb2 as errorpb
from tikv._proto import kvrpcpb_pb2 as kvrpcpb
from tikv._proto import metapb_pb2 as metapb
from tikv._proto import pdpb_pb2 as pdpb

CLUSTER_ID = 7_000_000_000_001


@dataclass
class FakeRegion:
    id: int
    start: bytes
    end: bytes
    stores: list[int]
    leader: int  # store id
    version: int = 1
    conf_ver: int = 1

    def contains(self, key: bytes) -> bool:
        return self.start <= key and (not self.end or key < self.end)

    def peer(self, store_id: int) -> metapb.Peer:
        return metapb.Peer(id=self.id * 100 + store_id, store_id=store_id)

    def meta(self) -> metapb.Region:
        return metapb.Region(
            id=self.id,
            start_key=self.start,
            end_key=self.end,
            region_epoch=metapb.RegionEpoch(conf_ver=self.conf_ver, version=self.version),
            peers=[self.peer(s) for s in self.stores],
        )


Fault = errorpb.Error | grpc.StatusCode | Callable[[], Any]


@dataclass
class FakeCluster:
    stores: int = 3
    credentials: grpc.ServerCredentials | None = None
    data: dict[bytes, tuple[bytes, float | None]] = field(default_factory=dict)
    calls: Counter[str] = field(default_factory=Counter)

    def __post_init__(self) -> None:
        self.lock = threading.RLock()
        self.regions: list[FakeRegion] = [FakeRegion(1, b"", b"", list(range(1, self.stores + 1)), leader=1)]
        self._next_id = 2
        self._faults: dict[str, deque[Fault]] = {}
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
        self.server.add_generic_rpc_handlers([self._pd_handler(), self._tikv_handler()])
        if self.credentials is None:
            self.address = f"127.0.0.1:{self.server.add_insecure_port('127.0.0.1:0')}"
        else:  # certificates name "localhost"
            self.address = f"localhost:{self.server.add_secure_port('localhost:0', self.credentials)}"
        self.server.start()

    def stop(self) -> None:
        self.server.stop(grace=None)

    # -- Test controls -----------------------------------------------------

    def split(self, *keys: bytes) -> None:
        """Split the regions containing *keys* at those keys."""
        with self.lock:
            for key in keys:
                region = self._region_for(key)
                if region.start == key:
                    continue
                right = FakeRegion(
                    self._next_id, key, region.end, list(region.stores), region.leader, region.version + 1
                )
                self._next_id += 1
                region.end = key
                region.version += 1
                self.regions.insert(self.regions.index(region) + 1, right)

    def merge(self, key: bytes) -> None:
        """Merge the region starting at *key* into the region before it."""
        with self.lock:
            right = self._region_for(key)
            assert right.start, "cannot merge the first region"
            assert right.start == key, "key must start a region"
            left = self.regions[self.regions.index(right) - 1]
            left.end = right.end
            left.version = max(left.version, right.version) + 1
            self.regions.remove(right)

    def transfer_leader(self, key: bytes, store_id: int) -> None:
        with self.lock:
            self._region_for(key).leader = store_id

    def fail(self, method: str, *faults: Fault) -> None:
        """Make the next calls of *method* fail with *faults*, in order.

        A fault is a region error, a gRPC status code, or a callable run
        before the request is served (e.g. to split a region mid-flight).
        """
        with self.lock:
            self._faults.setdefault(method, deque()).extend(faults)

    def region_count(self) -> int:
        return len(self.regions)

    # -- Internals -----------------------------------------------------------

    def _region_for(self, key: bytes) -> FakeRegion:
        starts = [r.start for r in self.regions]
        return self.regions[bisect.bisect_right(starts, key) - 1]

    def _by_id(self, region_id: int) -> FakeRegion | None:
        return next((r for r in self.regions if r.id == region_id), None)

    def _live(self, key: bytes) -> bytes | None:
        entry = self.data.get(key)
        if entry is None:
            return None
        value, expires = entry
        if expires is not None and expires <= time.monotonic():
            del self.data[key]
            return None
        return value

    def _set(self, key: bytes, value: bytes, ttl: int) -> None:
        self.data[key] = (value, time.monotonic() + ttl if ttl else None)

    def _injected(self, method: str, context: grpc.ServicerContext) -> errorpb.Error | None:
        queue = self._faults.get(method)
        while queue:
            fault = queue.popleft()
            if isinstance(fault, grpc.StatusCode):
                context.abort(fault, f"injected {fault.name}")
            if isinstance(fault, errorpb.Error):
                return fault
            fault()
        return None

    def _route(
        self, method: str, ctx: kvrpcpb.Context, keys: list[bytes], context: grpc.ServicerContext
    ) -> tuple[FakeRegion | None, errorpb.Error | None]:
        """Validate routing the way TiKV does; return the region or an error."""
        injected = self._injected(method, context)
        if injected is not None:
            return None, injected
        region = self._by_id(ctx.region_id)
        if region is None:
            return None, errorpb.Error(
                message="region not found", region_not_found=errorpb.RegionNotFound(region_id=ctx.region_id)
            )
        if ctx.peer.store_id != region.leader:
            return None, errorpb.Error(
                message="not leader",
                not_leader=errorpb.NotLeader(region_id=region.id, leader=region.peer(region.leader)),
            )
        epoch = ctx.region_epoch
        if (epoch.version, epoch.conf_ver) != (region.version, region.conf_ver):
            current = [r.meta() for r in self.regions if r.id == region.id or r.start == region.end]
            return None, errorpb.Error(
                message="epoch not match", epoch_not_match=errorpb.EpochNotMatch(current_regions=current)
            )
        for key in keys:
            if not region.contains(key):
                return None, errorpb.Error(
                    message="key not in region",
                    key_not_in_region=errorpb.KeyNotInRegion(
                        key=key, region_id=region.id, start_key=region.start, end_key=region.end
                    ),
                )
        return region, None

    # -- PD ------------------------------------------------------------------

    def _pd_handler(self) -> grpc.GenericRpcHandler:
        def check(request: Any, context: grpc.ServicerContext) -> None:
            if request.header.cluster_id != CLUSTER_ID:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "mismatch cluster id")

        def header() -> pdpb.ResponseHeader:
            return pdpb.ResponseHeader(cluster_id=CLUSTER_ID)

        def get_members(request: pdpb.GetMembersRequest, context: grpc.ServicerContext) -> Any:
            self._count("GetMembers", context)
            member = pdpb.Member(name="pd-0", member_id=1, client_urls=[f"http://{self.address}"])
            return pdpb.GetMembersResponse(header=header(), members=[member], leader=member)

        def region_response(region: FakeRegion | None) -> pdpb.GetRegionResponse:
            if region is None:
                return pdpb.GetRegionResponse(header=header())
            return pdpb.GetRegionResponse(
                header=header(), region=region.meta(), leader=region.peer(region.leader)
            )

        def get_region(request: pdpb.GetRegionRequest, context: grpc.ServicerContext) -> Any:
            self._count("GetRegion", context)
            check(request, context)
            with self.lock:
                return region_response(self._region_for(request.region_key))

        def get_prev_region(request: pdpb.GetRegionRequest, context: grpc.ServicerContext) -> Any:
            self._count("GetPrevRegion", context)
            check(request, context)
            with self.lock:
                # Like PD: the region before the one containing region_key.
                index = self.regions.index(self._region_for(request.region_key))
                return region_response(self.regions[index - 1] if index else None)

        def get_store(request: pdpb.GetStoreRequest, context: grpc.ServicerContext) -> Any:
            self._count("GetStore", context)
            check(request, context)
            store = metapb.Store(id=request.store_id, address=self.address)
            return pdpb.GetStoreResponse(header=header(), store=store)

        def scan_regions(request: pdpb.ScanRegionsRequest, context: grpc.ServicerContext) -> Any:
            self._count("ScanRegions", context)
            check(request, context)
            with self.lock:
                found = [
                    r
                    for r in self.regions
                    if (not r.end or r.end > request.start_key)
                    and (not request.end_key or r.start < request.end_key)
                ]
            if request.limit > 0:
                found = found[: request.limit]
            regions = [pdpb.Region(region=r.meta(), leader=r.peer(r.leader)) for r in found]
            return pdpb.ScanRegionsResponse(header=header(), regions=regions)

        return _handlers(
            "pdpb.PD",
            {
                "GetMembers": (get_members, pdpb.GetMembersRequest),
                "GetRegion": (get_region, pdpb.GetRegionRequest),
                "GetPrevRegion": (get_prev_region, pdpb.GetRegionRequest),
                "GetStore": (get_store, pdpb.GetStoreRequest),
                "ScanRegions": (scan_regions, pdpb.ScanRegionsRequest),
            },
        )

    def _count(self, method: str, context: grpc.ServicerContext) -> None:
        with self.lock:
            self.calls[method] += 1
            injected = self._injected(method, context)
        if injected is not None:
            raise AssertionError("PD methods only accept status-code faults")

    # -- TiKV ----------------------------------------------------------------

    def _tikv_handler(self) -> grpc.GenericRpcHandler:
        def serve(
            method: str, response_type: type, keys: Callable[[Any], list[bytes]], body: Callable
        ) -> Callable:
            def handler(request: Any, context: grpc.ServicerContext) -> Any:
                with self.lock:
                    self.calls[method] += 1
                    region, error = self._route(method, request.context, keys(request), context)
                    if error is not None:
                        return response_type(region_error=error)
                    return body(request, region)

            return handler

        def raw_get(req: kvrpcpb.RawGetRequest, region: FakeRegion) -> Any:
            value = self._live(req.key)
            return kvrpcpb.RawGetResponse(value=value or b"", not_found=value is None)

        def raw_batch_get(req: kvrpcpb.RawBatchGetRequest, region: FakeRegion) -> Any:
            pairs = [kvrpcpb.KvPair(key=k, value=v) for k in req.keys if (v := self._live(k)) is not None]
            return kvrpcpb.RawBatchGetResponse(pairs=pairs)

        def raw_put(req: kvrpcpb.RawPutRequest, region: FakeRegion) -> Any:
            self._set(req.key, req.value, req.ttl)
            return kvrpcpb.RawPutResponse()

        def raw_batch_put(req: kvrpcpb.RawBatchPutRequest, region: FakeRegion) -> Any:
            ttls = list(req.ttls)
            for i, pair in enumerate(req.pairs):
                ttl = ttls[0] if len(ttls) == 1 else (ttls[i] if ttls else 0)
                self._set(pair.key, pair.value, ttl)
            return kvrpcpb.RawBatchPutResponse()

        def raw_delete(req: kvrpcpb.RawDeleteRequest, region: FakeRegion) -> Any:
            self.data.pop(req.key, None)
            return kvrpcpb.RawDeleteResponse()

        def raw_batch_delete(req: kvrpcpb.RawBatchDeleteRequest, region: FakeRegion) -> Any:
            for key in req.keys:
                self.data.pop(key, None)
            return kvrpcpb.RawBatchDeleteResponse()

        def clamp(region: FakeRegion, lo: bytes, hi: bytes) -> tuple[bytes, bytes]:
            lo = max(lo, region.start)
            hi = region.end if not hi else (min(hi, region.end) if region.end else hi)
            return lo, hi

        def in_range(lo: bytes, hi: bytes) -> list[bytes]:
            return sorted(
                k for k in list(self.data) if k >= lo and (not hi or k < hi) and self._live(k) is not None
            )

        def raw_delete_range(req: kvrpcpb.RawDeleteRangeRequest, region: FakeRegion) -> Any:
            for key in in_range(*clamp(region, req.start_key, req.end_key)):
                del self.data[key]
            return kvrpcpb.RawDeleteRangeResponse()

        def raw_scan(req: kvrpcpb.RawScanRequest, region: FakeRegion) -> Any:
            upper = req.start_key
            if req.reverse and not (region.start < upper and (not region.end or upper <= region.end)):
                # Like TiKV: a reverse scan's upper bound must fall in the region.
                error = errorpb.KeyNotInRegion(key=upper, region_id=region.id, start_key=region.start)
                return kvrpcpb.RawScanResponse(
                    region_error=errorpb.Error(message="key not in region", key_not_in_region=error)
                )
            if req.reverse:
                # Like TiKV: an empty start key in a reverse scan matches nothing.
                keys = [] if not req.start_key else in_range(*clamp(region, req.end_key, req.start_key))[::-1]
            else:
                keys = in_range(*clamp(region, req.start_key, req.end_key))
            keys = keys[: req.limit]
            kvs = [kvrpcpb.KvPair(key=k, value=b"" if req.key_only else self.data[k][0]) for k in keys]
            return kvrpcpb.RawScanResponse(kvs=kvs)

        def raw_get_key_ttl(req: kvrpcpb.RawGetKeyTTLRequest, region: FakeRegion) -> Any:
            if self._live(req.key) is None:
                return kvrpcpb.RawGetKeyTTLResponse(not_found=True)
            expires = self.data[req.key][1]
            ttl = 0 if expires is None else math.ceil(expires - time.monotonic())
            return kvrpcpb.RawGetKeyTTLResponse(ttl=ttl)

        def raw_cas(req: kvrpcpb.RawCASRequest, region: FakeRegion) -> Any:
            current = self._live(req.key)
            matches = current is None if req.previous_not_exist else current == req.previous_value
            if matches:
                self._set(req.key, req.value, req.ttl)
            return kvrpcpb.RawCASResponse(
                succeed=matches, previous_not_exist=current is None, previous_value=current or b""
            )

        def raw_checksum(req: kvrpcpb.RawChecksumRequest, region: FakeRegion) -> Any:
            checksum = kvs = size = 0
            for key_range in req.ranges:
                for k in in_range(*clamp(region, key_range.start_key, key_range.end_key)):
                    v = self.data[k][0]
                    checksum ^= zlib.crc32(k + b"=" + v)
                    kvs += 1
                    size += len(k) + len(v)
            return kvrpcpb.RawChecksumResponse(checksum=checksum, total_kvs=kvs, total_bytes=size)

        def key(req: Any) -> list[bytes]:
            return [req.key]

        def keys(req: Any) -> list[bytes]:
            return list(req.keys)

        def pair_keys(req: Any) -> list[bytes]:
            return [p.key for p in req.pairs]

        def range_keys(req: Any) -> list[bytes]:
            return [r.start_key for r in req.ranges]

        def range_start(req: Any) -> list[bytes]:
            return [req.start_key] if not getattr(req, "reverse", False) else []

        k = kvrpcpb
        table = {
            "RawGet": (k.RawGetRequest, k.RawGetResponse, key, raw_get),
            "RawBatchGet": (k.RawBatchGetRequest, k.RawBatchGetResponse, keys, raw_batch_get),
            "RawPut": (k.RawPutRequest, k.RawPutResponse, key, raw_put),
            "RawBatchPut": (k.RawBatchPutRequest, k.RawBatchPutResponse, pair_keys, raw_batch_put),
            "RawDelete": (k.RawDeleteRequest, k.RawDeleteResponse, key, raw_delete),
            "RawBatchDelete": (k.RawBatchDeleteRequest, k.RawBatchDeleteResponse, keys, raw_batch_delete),
            "RawDeleteRange": (
                k.RawDeleteRangeRequest,
                k.RawDeleteRangeResponse,
                range_start,
                raw_delete_range,
            ),
            "RawScan": (k.RawScanRequest, k.RawScanResponse, range_start, raw_scan),
            "RawGetKeyTTL": (k.RawGetKeyTTLRequest, k.RawGetKeyTTLResponse, key, raw_get_key_ttl),
            "RawCompareAndSwap": (k.RawCASRequest, k.RawCASResponse, key, raw_cas),
            "RawChecksum": (k.RawChecksumRequest, k.RawChecksumResponse, range_keys, raw_checksum),
        }
        return _handlers(
            "tikvpb.Tikv",
            {
                name: (serve(name, response, extract, body), request)
                for name, (request, response, extract, body) in table.items()
            },
        )


def _handlers(service: str, methods: dict[str, tuple[Callable, type]]) -> grpc.GenericRpcHandler:
    return grpc.method_handlers_generic_handler(
        service,
        {
            name: grpc.unary_unary_rpc_method_handler(
                fn,
                request_deserializer=request.FromString,
                response_serializer=lambda m: m.SerializeToString(),
            )
            for name, (fn, request) in methods.items()
        },
    )
