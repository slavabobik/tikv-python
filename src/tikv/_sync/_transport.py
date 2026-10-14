# Generated from _async/_transport.py by scripts/unasync.py. DO NOT EDIT.
"""A pool of gRPC channels, one per server address."""

from __future__ import annotations

import threading
from typing import Any

import grpc
from google.protobuf.message import Message

from .._rpc import CHANNEL_OPTIONS, Method, Response
from ._compat import Channel, open_channel


class Transport:
    """Sends unary requests to PD and TiKV servers over shared channels."""

    def __init__(self, credentials: grpc.ChannelCredentials | None = None) -> None:
        self._credentials = credentials
        self._lock = threading.Lock()  # never held across an await
        self._channels: dict[str, Channel] = {}
        self._stubs: dict[tuple[str, str], Any] = {}

    def call(
        self, address: str, method: Method[Response], request: Message, timeout: float
    ) -> Response:
        stub = self._stubs.get((address, method.path))
        if stub is None:
            stub = self._channel(address).unary_unary(
                method.path,
                request_serializer=type(request).SerializeToString,
                response_deserializer=method.response.FromString,
            )
            self._stubs[address, method.path] = stub
        response: Response = stub(request, timeout=timeout)
        return response

    def _channel(self, address: str) -> Channel:
        with self._lock:
            channel = self._channels.get(address)
            if channel is None:
                channel = open_channel(address, self._credentials, CHANNEL_OPTIONS)
                self._channels[address] = channel
            return channel

    def close(self) -> None:
        with self._lock:
            channels = list(self._channels.values())
            self._channels.clear()
            self._stubs.clear()
        for channel in channels:
            channel.close()
