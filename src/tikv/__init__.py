"""A pure-Python client for TiKV's raw key-value API.

Blocking::

    import tikv

    with tikv.RawClient(["127.0.0.1:2379"]) as client:
        client.put(b"greeting", b"hello")
        client.get(b"greeting")            # b"hello"

asyncio::

    async with tikv.AsyncRawClient(["127.0.0.1:2379"]) as client:
        await client.put(b"greeting", b"hello")
"""

from ._async._client import AsyncRawClient
from ._errors import (
    PDError,
    RegionError,
    ServerError,
    TiKVError,
    TimeoutError,
    UndeterminedError,
)
from ._rpc import TLSConfig
from ._sync._client import RawClient
from ._types import CasResult, Checksum

__all__ = [
    "AsyncRawClient",
    "CasResult",
    "Checksum",
    "PDError",
    "RawClient",
    "RegionError",
    "ServerError",
    "TLSConfig",
    "TiKVError",
    "TimeoutError",
    "UndeterminedError",
]

__version__ = "0.1.0"
