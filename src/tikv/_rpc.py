"""gRPC method descriptors and channel settings for PD and TiKV."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Generic, TypeVar

import grpc
from google.protobuf.message import Message

from ._proto import kvrpcpb_pb2 as kvrpcpb
from ._proto import pdpb_pb2 as pdpb

Response = TypeVar("Response", bound=Message)


@dataclass(frozen=True, slots=True)
class Method(Generic[Response]):
    """A unary gRPC method: its full path and response message type."""

    path: str
    response: type[Response]


def _tikv(name: str, response: type[Response]) -> Method[Response]:
    return Method(f"/tikvpb.Tikv/{name}", response)


def _pd(name: str, response: type[Response]) -> Method[Response]:
    return Method(f"/pdpb.PD/{name}", response)


RAW_GET = _tikv("RawGet", kvrpcpb.RawGetResponse)
RAW_BATCH_GET = _tikv("RawBatchGet", kvrpcpb.RawBatchGetResponse)
RAW_PUT = _tikv("RawPut", kvrpcpb.RawPutResponse)
RAW_BATCH_PUT = _tikv("RawBatchPut", kvrpcpb.RawBatchPutResponse)
RAW_DELETE = _tikv("RawDelete", kvrpcpb.RawDeleteResponse)
RAW_BATCH_DELETE = _tikv("RawBatchDelete", kvrpcpb.RawBatchDeleteResponse)
RAW_DELETE_RANGE = _tikv("RawDeleteRange", kvrpcpb.RawDeleteRangeResponse)
RAW_SCAN = _tikv("RawScan", kvrpcpb.RawScanResponse)
RAW_GET_KEY_TTL = _tikv("RawGetKeyTTL", kvrpcpb.RawGetKeyTTLResponse)
RAW_COMPARE_AND_SWAP = _tikv("RawCompareAndSwap", kvrpcpb.RawCASResponse)
RAW_CHECKSUM = _tikv("RawChecksum", kvrpcpb.RawChecksumResponse)

PD_GET_MEMBERS = _pd("GetMembers", pdpb.GetMembersResponse)
PD_GET_REGION = _pd("GetRegion", pdpb.GetRegionResponse)
PD_GET_PREV_REGION = _pd("GetPrevRegion", pdpb.GetRegionResponse)
PD_GET_STORE = _pd("GetStore", pdpb.GetStoreResponse)
PD_SCAN_REGIONS = _pd("ScanRegions", pdpb.ScanRegionsResponse)


CHANNEL_OPTIONS = (
    # Scans and batch reads can legitimately exceed gRPC's 4 MiB default.
    ("grpc.max_receive_message_length", -1),
    ("grpc.max_send_message_length", -1),
    # Detect dead peers quickly, like client-go does.
    ("grpc.keepalive_time_ms", 10_000),
    ("grpc.keepalive_timeout_ms", 3_000),
    ("grpc.keepalive_permit_without_calls", 1),
    ("grpc.http2.max_pings_without_data", 0),
)


@dataclass(frozen=True, slots=True)
class TLSConfig:
    """Paths to PEM files for mutual TLS with PD and TiKV.

    >>> TLSConfig(ca="ca.pem", cert="client.pem", key="client-key.pem")  # doctest: +SKIP
    """

    ca: str | os.PathLike[str]
    cert: str | os.PathLike[str] | None = None
    key: str | os.PathLike[str] | None = None

    def credentials(self) -> grpc.ChannelCredentials:
        def read(path: str | os.PathLike[str] | None) -> bytes | None:
            if path is None:
                return None
            with open(path, "rb") as file:
                return file.read()

        return grpc.ssl_channel_credentials(
            root_certificates=read(self.ca),
            private_key=read(self.key),
            certificate_chain=read(self.cert),
        )


def strip_scheme(url: str) -> str:
    """``http://host:2379`` -> ``host:2379``; PD advertises URLs with schemes."""
    return url.split("://", 1)[-1].rstrip("/")


# Status codes that say "this will never work", as opposed to transient
# network or availability problems that a retry can fix.
_FATAL_CODES = frozenset(
    {
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.UNIMPLEMENTED,
        grpc.StatusCode.UNAUTHENTICATED,
        grpc.StatusCode.PERMISSION_DENIED,
    }
)


def is_retryable(error: grpc.RpcError) -> bool:
    code = error.code() if callable(getattr(error, "code", None)) else None
    return code not in _FATAL_CODES
