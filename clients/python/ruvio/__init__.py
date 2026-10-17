# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Python client for Ruvio, a Redis-compatible server with extra native commands.

Two clients share the same typed command methods:

* ``ruvio.Ruvio`` - synchronous client (one socket, not thread-safe).
* ``ruvio.asyncio.Ruvio`` - asyncio client (one pipelined, multiplexed connection shared by every
  coroutine). Its command methods return awaitables.

``ruvio.ClusterRuvio`` (and ``ruvio.asyncio.ClusterRuvio``) route each key to the shard that owns it;
``Ruvio(..., cluster=True)`` builds one. Reading help for a command: ``help(ruvio.Ruvio.set)`` or hover
in your IDE; each method lists its arguments, return value, errors and an example.

Also exported: :func:`key_slot` (hash slot of a key), :func:`parse_url`, :func:`parse_info`, the result
tuples (:class:`FillResult`, :class:`IdemResult`, :class:`GeoResult`, :class:`StreamEntry`,
:class:`PendingSummary`, :class:`PendingEntry`, :class:`AutoClaimResult`, :class:`Message`) and the
exceptions, all deriving from :class:`RuvioError`.

Example:
    from ruvio import Ruvio

    with Ruvio("127.0.0.1", 6379) as client:
        client.set("greeting", "hello", ex=60)
        print(client.get("greeting"))
"""

from ._commands import parse_info
from ._constants import (
    Aggregate,
    BitFieldOp,
    BitOperation,
    BitUnit,
    GeoOrder,
    GeoUnit,
    InsertPosition,
    KeyType,
    ListSide,
    Overflow,
    bit_type,
    signed,
    unsigned,
)
from ._models import (
    AutoClaimResult,
    FillResult,
    GeoResult,
    IdemResult,
    Message,
    PendingEntry,
    PendingSummary,
    StreamEntry,
)
from ._cluster import key_slot
from .client import Lock, Pipeline, PubSub, Ruvio, parse_url
from .cluster import ClusterRuvio
from .errors import (
    BusyError,
    ConnectionError,
    CrossSlotError,
    ExecAbortError,
    LockError,
    MovedError,
    NoGroupError,
    NoScriptError,
    ProtocolError,
    ResponseError,
    RuvioError,
    TimeoutError,
    WatchError,
    WrongTypeError,
)

__version__ = "0.1.1"

__all__ = [
    "Ruvio",
    "ClusterRuvio",
    "key_slot",
    "Pipeline",
    "PubSub",
    "Lock",
    "parse_url",
    "parse_info",
    "Aggregate",
    "BitFieldOp",
    "BitOperation",
    "BitUnit",
    "GeoOrder",
    "GeoUnit",
    "InsertPosition",
    "KeyType",
    "ListSide",
    "Overflow",
    "bit_type",
    "signed",
    "unsigned",
    "AutoClaimResult",
    "FillResult",
    "GeoResult",
    "IdemResult",
    "Message",
    "PendingEntry",
    "PendingSummary",
    "StreamEntry",
    "BusyError",
    "ConnectionError",
    "CrossSlotError",
    "ExecAbortError",
    "LockError",
    "MovedError",
    "NoGroupError",
    "NoScriptError",
    "ProtocolError",
    "ResponseError",
    "RuvioError",
    "TimeoutError",
    "WatchError",
    "WrongTypeError",
    "__version__",
]
