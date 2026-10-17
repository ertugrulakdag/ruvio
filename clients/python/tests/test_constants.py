# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Keyword enums: value tables and wire bytes equal to the plain-string path (no server needed)."""

from __future__ import annotations

import pytest

import ruvio
from ruvio import (
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
    signed,
    unsigned,
)
from ruvio._resp import encode, encode_args

from test_command_encoding import Rec

TABLES = [
    (GeoUnit, ["m", "km", "ft", "mi"]),
    (GeoOrder, ["ASC", "DESC"]),
    (InsertPosition, ["BEFORE", "AFTER"]),
    (ListSide, ["LEFT", "RIGHT"]),
    (BitOperation, ["AND", "OR", "XOR", "NOT"]),
    (BitUnit, ["BYTE", "BIT"]),
    (Aggregate, ["SUM", "MIN", "MAX"]),
    (KeyType, ["string", "json", "list", "set", "hash", "zset", "bloomfilter", "stream", "lease", "limit", "semaphore", "once"]),
    (BitFieldOp, ["GET", "SET", "INCRBY"]),
    (Overflow, ["WRAP", "SAT", "FAIL"]),
]


@pytest.mark.parametrize("cls,values", TABLES)
def test_value_tables(cls, values):
    assert [m.value for m in cls] == values
    for m in cls:
        assert isinstance(m, str) and m == m.value and str(m) == m.value and f"{m}" == m.value
        assert m.wire == m.value.encode()
        assert getattr(ruvio, cls.__name__) is cls


@pytest.mark.parametrize("cls,values", TABLES)
def test_encoding_equals_string_path(cls, values):
    for m in cls:
        assert encode("X", ("k", m, 1)) == encode("X", ("k", m.value, 1))
        assert encode("X", (m,)) == encode("X", (m.value,))
        assert encode_args(("X", m)) == encode_args(("X", m.value))


def test_bit_types():
    assert signed(8) == "i8" and signed(64) == "i64" and unsigned(1) == "u1" and unsigned(63) == "u63"
    for bad in (0, 65):
        with pytest.raises(ValueError):
            signed(bad)
    with pytest.raises(ValueError):
        unsigned(64)


def _same(call, reply=None):
    a, b = Rec(reply), Rec(reply)
    call(a, True)
    call(b, False)
    assert a.frame == b.frame


def test_commands_accept_enum_and_str():
    _same(lambda r, e: r.geodist("k", "a", "b", GeoUnit.KILOMETERS if e else "km"))
    _same(lambda r, e: r.geosearch("k", member="a", radius=5, unit=GeoUnit.MILES if e else "mi", sort=GeoOrder.DESC if e else "DESC"))
    _same(lambda r, e: r.georadius("k", 1.0, 2.0, 5, unit=GeoUnit.FEET if e else "ft", sort=GeoOrder.ASC if e else "ASC"))
    _same(lambda r, e: r.linsert("k", InsertPosition.BEFORE if e else "BEFORE", "p", "v"))
    _same(lambda r, e: r.lmove("a", "b", ListSide.RIGHT if e else "RIGHT", ListSide.LEFT if e else "LEFT"))
    _same(lambda r, e: r.blmove("a", "b", 1, ListSide.RIGHT if e else "RIGHT", ListSide.LEFT if e else "LEFT"))
    _same(lambda r, e: r.lmpop(["a"], ListSide.RIGHT if e else "RIGHT", 2))
    _same(lambda r, e: r.blmpop(1, ["a"], ListSide.RIGHT if e else "RIGHT"))
    _same(lambda r, e: r.scan(0, type=KeyType.ZSET if e else "zset"), [b"0", []])
    _same(lambda r, e: r.bitop(BitOperation.XOR if e else "XOR", "d", "a", "b"))
    _same(lambda r, e: r.bitcount("k", 0, 7, BitUnit.BIT if e else "BIT"))
    _same(lambda r, e: r.zunionstore("d", ["a", "b"], aggregate=Aggregate.MAX if e else "MAX"))
    _same(
        lambda r, e: r.bitfield("k", "OVERFLOW", Overflow.SAT if e else "SAT", BitFieldOp.INCRBY if e else "INCRBY", unsigned(8), 0, 10)
    )
