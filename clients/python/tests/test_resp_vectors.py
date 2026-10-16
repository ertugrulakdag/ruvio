# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""The RESP wire vectors in clients/testdata/resp.json are shared by every client, so the six
encoders and parsers cannot drift apart. Runs the pure-Python parser, and the hiredis one when
it is installed, each with decoding on and off."""

from __future__ import annotations

import json
import os

import pytest

from ruvio import _converters, _resp
from ruvio._resp import NEED, encode, encode_args
from ruvio.errors import ResponseError, make_error

VECTORS = os.path.join(os.path.dirname(__file__), "..", "..", "testdata", "resp.json")

with open(VECTORS, encoding="utf-8") as _f:
    _DATA = json.load(_f)

KINDS = ["python"] + (["hiredis"] if _resp.hiredis_available() else [])


def _wire(v):
    return bytes.fromhex(v["bytes_hex"]) if "bytes_hex" in v else v["bytes"].encode("utf-8")


def _expected(shape, kind, decode):
    """The neutral value shape as this parser returns it."""
    [(tag, inner)] = shape.items()
    if tag == "simple":
        # hiredis cannot tell a status reply from a bulk, so undecoded it returns bytes too.
        return inner.encode() if kind == "hiredis" and not decode else inner
    if tag == "error":
        return make_error(inner)
    if tag == "int":
        return int(inner)
    if tag in ("bulk", "bulk_hex"):
        raw = inner.encode("utf-8") if tag == "bulk" else bytes.fromhex(inner)
        return raw.decode("utf-8", "surrogateescape") if decode else raw
    if tag == "null":
        return None
    assert tag == "array", tag
    return [_expected(item, kind, decode) for item in inner]


def _same(got, want, name):
    if isinstance(want, ResponseError):
        assert type(got) is type(want) and str(got) == str(want), name
    elif isinstance(want, list):
        assert type(got) is list and len(got) == len(want), name
        for g, w in zip(got, want):
            _same(g, w, name)
    else:
        assert type(got) is type(want) and got == want, f"{name}: {got!r} != {want!r}"


@pytest.mark.parametrize("v", _DATA["encode"], ids=lambda v: v["name"])
def test_encoder_writes_the_shared_vectors(v):
    want = _wire(v)
    if "args" in v:
        args = tuple(v["args"])
        assert encode_args(args) == want
        assert encode(args[0], args[1:]) == want
        raw = tuple(a.encode() for a in args)
    else:
        raw = tuple(bytes.fromhex(a) for a in v["args_hex"])
    assert encode_args(raw) == want
    assert encode(raw[0].decode(), raw[1:]) == want


@pytest.mark.parametrize("decode", [True, False])
@pytest.mark.parametrize("kind", KINDS)
def test_parser_reads_the_shared_vectors(kind, decode):
    assert _DATA["decode"]
    for v in _DATA["decode"]:
        p = _resp.make_parser(kind, decode=decode)
        p.feed(_wire(v))
        _same(p.gets(), _expected(v["value"], kind, decode), v["name"])
        assert not p.has_data(), f"{v['name']}: bytes left over"


@pytest.mark.parametrize("kind", KINDS)
def test_parser_asks_for_more_data_on_an_incomplete_reply(kind):
    assert _DATA["incomplete"]
    for v in _DATA["incomplete"]:
        p = _resp.make_parser(kind)
        p.feed(_wire(v))
        assert p.gets() is NEED, v["name"]


@pytest.mark.parametrize("decode", [True, False])
@pytest.mark.parametrize("kind", KINDS)
def test_ok_reply_is_success_with_every_parser(kind, decode):
    p = _resp.make_parser(kind, decode=decode)
    p.feed(b"+OK\r\n+OK\r\n")
    assert _converters._ok(p.gets()) and _converters._set_reply(p.gets())
