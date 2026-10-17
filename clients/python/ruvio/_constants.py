# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Typed keyword constants for command arguments.

Every class is a ``str`` enum, so a member can be passed anywhere the plain string is
accepted (``unit=GeoUnit.KILOMETERS`` and ``unit="km"`` send the same bytes) and
existing calls keep working. Each member carries its wire bytes, built once at import;
the RESP encoder reads them directly, so a member costs no more than a ``bytes``
argument (no ``str`` conversion, no encode, no allocation per call).
"""

from __future__ import annotations

from enum import Enum
from typing import Set

__all__ = [
    "Keyword",
    "GeoUnit",
    "GeoOrder",
    "InsertPosition",
    "ListSide",
    "BitOperation",
    "BitUnit",
    "Aggregate",
    "KeyType",
    "BitFieldOp",
    "Overflow",
    "bit_type",
    "signed",
    "unsigned",
]

KEYWORD_TYPES: Set[type] = set()
"""Every keyword enum class; the encoder checks ``type(arg) in KEYWORD_TYPES``."""


class Keyword(str, Enum):
    """Base of the keyword enums: a ``str`` whose wire bytes are encoded once."""

    def __new__(cls, value: str) -> "Keyword":
        obj = str.__new__(cls, value)
        obj._value_ = value
        obj._wire = value.encode()  # type: ignore[attr-defined]
        return obj

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        KEYWORD_TYPES.add(cls)

    def __str__(self) -> str:
        return self._value_  # type: ignore[no-any-return]

    __format__ = str.__format__

    @property
    def wire(self) -> bytes:
        """The pre-encoded argument bytes (``b"km"``)."""
        return self._wire  # type: ignore[attr-defined, no-any-return]


class GeoUnit(Keyword):
    """Distance unit of ``GEODIST``, ``GEOSEARCH*`` and ``GEORADIUS*``: ``"m"``, ``"km"``, ``"ft"`` or ``"mi"``.

    Example: ``r.geosearch("cities", member="A", radius=5, unit=GeoUnit.KILOMETERS)``.
    """

    METERS = "m"
    KILOMETERS = "km"
    FEET = "ft"
    MILES = "mi"


class GeoOrder(Keyword):
    """Result order of the GEO search commands (``sort=``): ``"ASC"`` or ``"DESC"``.

    Example: ``r.geosearch("cities", member="A", radius=5, sort=GeoOrder.ASC)``.
    """

    ASC = "ASC"
    DESC = "DESC"


class InsertPosition(Keyword):
    """Where ``LINSERT`` puts the value relative to the pivot: ``"BEFORE"`` or ``"AFTER"``.

    Example: ``r.linsert("q", InsertPosition.BEFORE, "b", "a")``.
    """

    BEFORE = "BEFORE"
    AFTER = "AFTER"


class ListSide(Keyword):
    """End of a list for ``LMOVE``, ``BLMOVE``, ``LMPOP`` and ``BLMPOP``: ``"LEFT"`` or ``"RIGHT"``.

    Example: ``r.lmove("src", "dst", ListSide.LEFT, ListSide.RIGHT)``.
    """

    LEFT = "LEFT"
    RIGHT = "RIGHT"


class BitOperation(Keyword):
    """Operation of ``BITOP``: ``"AND"``, ``"OR"``, ``"XOR"`` or ``"NOT"``.

    Example: ``r.bitop(BitOperation.AND, "dest", "a", "b")``.
    """

    AND = "AND"
    OR = "OR"
    XOR = "XOR"
    NOT = "NOT"


class BitUnit(Keyword):
    """Range unit of ``BITCOUNT`` (``mode=``): ``"BYTE"`` or ``"BIT"``.

    Example: ``r.bitcount("k", 0, 7, BitUnit.BIT)``.
    """

    BYTE = "BYTE"
    BIT = "BIT"


class Aggregate(Keyword):
    """Score combination of ``ZUNION*`` / ``ZINTER*`` (``aggregate=``): ``"SUM"``, ``"MIN"`` or ``"MAX"``.

    Example: ``r.zunionstore("dst", ["a", "b"], aggregate=Aggregate.MAX)``.
    """

    SUM = "SUM"
    MIN = "MIN"
    MAX = "MAX"


class KeyType(Keyword):
    """Value of ``SCAN ... TYPE`` (``type=``): ``string``, ``json``, ``list``, ``set``, ``hash``, ``zset``,
    ``bloomfilter``, ``stream``, ``lease``, ``limit``, ``semaphore`` or ``once``.

    Example: ``r.scan(0, type=KeyType.HASH)``.
    """

    STRING = "string"
    JSON = "json"
    LIST = "list"
    SET = "set"
    HASH = "hash"
    ZSET = "zset"
    BLOOM_FILTER = "bloomfilter"
    STREAM = "stream"
    LEASE = "lease"
    LIMIT = "limit"
    SEMAPHORE = "semaphore"
    ONCE = "once"


class BitFieldOp(Keyword):
    """Sub-command word of ``BITFIELD``: ``"GET"``, ``"SET"`` or ``"INCRBY"``.

    Example: ``r.bitfield("k", BitFieldOp.INCRBY, unsigned(8), 0, 10)``.
    """

    GET = "GET"
    SET = "SET"
    INCRBY = "INCRBY"


class Overflow(Keyword):
    """Overflow policy after ``BITFIELD ... OVERFLOW``: ``"WRAP"``, ``"SAT"`` or ``"FAIL"``.

    Example: ``r.bitfield("k", "OVERFLOW", Overflow.SAT, BitFieldOp.INCRBY, unsigned(8), 0, 10)``.
    """

    WRAP = "WRAP"
    SAT = "SAT"
    FAIL = "FAIL"


_SIGNED = tuple(f"i{n}" for n in range(0, 65))
_UNSIGNED = tuple(f"u{n}" for n in range(0, 64))


def signed(bits: int) -> str:
    """``BITFIELD`` type word for a signed integer of ``bits`` bits (1-64): ``signed(8)`` is ``"i8"``."""
    if not 1 <= bits <= 64:
        raise ValueError("signed bit width must be 1-64")
    return _SIGNED[bits]


def unsigned(bits: int) -> str:
    """``BITFIELD`` type word for an unsigned integer of ``bits`` bits (1-63): ``unsigned(8)`` is ``"u8"``."""
    if not 1 <= bits <= 63:
        raise ValueError("unsigned bit width must be 1-63")
    return _UNSIGNED[bits]


def bit_type(bits: int, is_signed: bool = False) -> str:
    """``BITFIELD`` type word: ``bit_type(8)`` is ``"u8"``, ``bit_type(16, True)`` is ``"i16"``."""
    return signed(bits) if is_signed else unsigned(bits)
