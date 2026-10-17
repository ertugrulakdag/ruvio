# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Python client for Ruvio.

``ruvio.Ruvio`` is the synchronous client; ``ruvio.asyncio.Ruvio`` is the asyncio client
(one pipelined, multiplexed connection shared by every coroutine).
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
