# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Small result types returned by typed commands."""

from __future__ import annotations

from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Union

__all__ = [
    "FillResult",
    "IdemResult",
    "StreamEntry",
    "GeoResult",
    "PendingSummary",
    "PendingEntry",
    "AutoClaimResult",
    "Message",
    "Value",
]

Value = Union[str, bytes]
"""A reply string: ``str`` on a decoding client, ``bytes`` on ``client.raw`` / ``decode_responses=False``."""


class FillResult(NamedTuple):
    """``FILLGET`` outcome. ``state`` is ``HIT``, ``FILL``, ``BUSY`` or ``STALE``.

    ``HIT``: ``value`` is the cached value. ``FILL``: this caller must build the value
    and call ``FILLSET`` with ``token``. ``BUSY``: someone else is filling. ``STALE``:
    ``value`` is a stale copy and ``token`` (when not None) makes this caller the refresher.
    """

    state: str
    value: Optional[Value]
    token: Optional[Value]


class IdemResult(NamedTuple):
    """``IDEM BEGIN`` outcome: ``acquired``, ``pending``, ``conflict`` or ``completed`` (+ result)."""

    state: str
    result: Optional[Value]


class StreamEntry(NamedTuple):
    id: str
    fields: Dict[Any, Any]


class GeoResult(NamedTuple):
    """One ``GEOSEARCH``/``GEORADIUS`` row when WITHDIST/WITHHASH/WITHCOORD is used."""

    member: Value
    dist: Optional[float]
    hash: Optional[int]
    coord: Optional[Tuple[float, float]]


class PendingSummary(NamedTuple):
    count: int
    min_id: Optional[str]
    max_id: Optional[str]
    consumers: List[Tuple[str, int]]


class PendingEntry(NamedTuple):
    id: str
    consumer: str
    idle_ms: int
    deliveries: int


class AutoClaimResult(NamedTuple):
    next_id: str
    claimed: List[Any]
    deleted: List[str]


class Message(NamedTuple):
    """A Pub/Sub delivery. ``kind`` is ``message``, ``pmessage``, ``smessage``,
    ``subscribe``, ``unsubscribe``, ``psubscribe``, ``punsubscribe``, ``ssubscribe``,
    ``sunsubscribe`` or ``pong``."""

    kind: str
    channel: Optional[Value]
    data: Any
    pattern: Optional[Value] = None
