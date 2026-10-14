"""Public result types."""

from __future__ import annotations

from typing import NamedTuple


class Checksum(NamedTuple):
    """Outcome of :meth:`RawClient.checksum`: a fingerprint of a key range.

    Equal ranges have equal checksums, which makes this handy for verifying
    copies and migrations without reading the data back.
    """

    #: CRC64 of every key-value pair, XOR-ed together.
    crc64_xor: int
    #: Number of keys in the range.
    keys: int
    #: Total size of the keys and values, in bytes.
    size: int


class CasResult(NamedTuple):
    """Outcome of :meth:`RawClient.compare_and_swap`.

    Unpacks naturally::

        swapped, previous = client.compare_and_swap(key, b"old", b"new")
    """

    #: Whether the new value was written.
    swapped: bool
    #: The value before the operation, or ``None`` if the key did not exist.
    previous: bytes | None
