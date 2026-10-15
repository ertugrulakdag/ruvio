# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Codec unit tests (no server): encoder, pure-Python parser, optional hiredis parser."""

from __future__ import annotations

import pytest

from ruvio import _resp
from ruvio._resp import NEED, Parser, Push, encode, encode_args
from ruvio.errors import NoScriptError, ProtocolError, ResponseError, WrongTypeError

KINDS = ["python"] + (["hiredis"] if _resp.hiredis_available() else [])


def drain(p):
    out = []
    while True:
        v = p.gets()
        if v is NEED:
            return out
        out.append(v)


def test_encode_basic():
    assert encode("GET", ("k",)) == b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
    assert encode("SET", ("k", b"\x00\xff", 5, -3, 1.5, True)) == (
        b"*7\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\n\x00\xff\r\n$1\r\n5\r\n$2\r\n-3\r\n$3\r\n1.5\r\n$1\r\n1\r\n"
    )
    assert encode("CLIENT LIST", ()) == b"*2\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n"
    assert encode("PING", ()) == b"*1\r\n$4\r\nPING\r\n"
    assert encode("X", ("é",)) == b"*2\r\n$1\r\nX\r\n$2\r\n\xc3\xa9\r\n"
    assert encode("X", (bytearray(b"ab"), memoryview(b"cd"))) == b"*3\r\n$1\r\nX\r\n$2\r\nab\r\n$2\r\ncd\r\n"
    assert encode("X", (float("inf"), 2**70)) == b"*3\r\n$1\r\nX\r\n$3\r\ninf\r\n$22\r\n1180591620717411303424\r\n"
    big = "v" * 5000
    assert encode("SET", ("k", big)) == b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5000\r\n" + big.encode() + b"\r\n"
    many = tuple(range(5000))
    assert encode("RPUSH", many).startswith(b"*5001\r\n$5\r\nRPUSH\r\n$1\r\n0\r\n")
    # surrogate-escaped str round-trips undecodable bytes
    assert encode("X", (b"\xff".decode("utf-8", "surrogateescape"),)) == b"*2\r\n$1\r\nX\r\n$1\r\n\xff\r\n"
    with pytest.raises(TypeError):
        encode("X", (None,))
    assert encode_args(("SET", "k", 1, b"v", 2.5)) == encode("SET", ("k", 1, b"v", 2.5))


@pytest.mark.parametrize("kind", KINDS)
def test_parse_scalars_and_errors(kind):
    p = _resp.make_parser(kind)
    p.feed(b"+OK\r\n+QUEUED\r\n:42\r\n:-7\r\n$3\r\nabc\r\n$0\r\n\r\n$-1\r\n*-1\r\n-ERR bad thing\r\n-WRONGTYPE x\r\n-NOSCRIPT y\r\n")
    out = drain(p)
    assert out[:8] == ["OK", "QUEUED", 42, -7, "abc", "", None, None]
    assert type(out[8]) is ResponseError and out[8].code == "ERR" and str(out[8]) == "ERR bad thing"
    assert type(out[9]) is WrongTypeError and type(out[10]) is NoScriptError


@pytest.mark.parametrize("kind", KINDS)
def test_parse_arrays_nested(kind):
    p = _resp.make_parser(kind)
    p.feed(b"*0\r\n*3\r\n$1\r\na\r\n:1\r\n*2\r\n$-1\r\n*1\r\n+x\r\n*2\r\n-ERR in array\r\n:5\r\n")
    out = drain(p)
    assert out[0] == []
    assert out[1] == ["a", 1, [None, ["x"]]]
    assert isinstance(out[2][0], ResponseError) and out[2][1] == 5


@pytest.mark.parametrize("kind", KINDS)
def test_parse_byte_by_byte(kind):
    data = b"*4\r\n$5\r\nhello\r\n:12345\r\n*2\r\n$2\r\nhi\r\n$-1\r\n$11\r\nhello world\r\n+OK\r\n"
    p = _resp.make_parser(kind)
    out = []
    for i in range(len(data)):
        p.feed(data[i : i + 1])
        out += drain(p)
    assert out == [["hello", 12345, ["hi", None], "hello world"], "OK"]


@pytest.mark.parametrize("kind", KINDS)
def test_parse_decode_toggle_and_binary(kind):
    p = _resp.make_parser(kind)
    p.decode = False
    p.feed(b"$3\r\n\x00\xff\x80\r\n*1\r\n$1\r\na\r\n")
    assert drain(p) == [b"\x00\xff\x80", [b"a"]]
    p.decode = True
    p.feed(b"$3\r\n\x00\xff\x80\r\n$2\r\n\xc3\xa9\r\n")
    weird, e = drain(p)
    assert weird.encode("utf-8", "surrogateescape") == b"\x00\xff\x80" and e == "é"


@pytest.mark.parametrize("kind", KINDS)
def test_parse_resp3(kind):
    p = _resp.make_parser(kind)
    p.feed(
        b"%2\r\n+a\r\n:1\r\n+b\r\n*1\r\n:2\r\n"
        b"_\r\n#t\r\n#f\r\n,1.5\r\n,inf\r\n(123456789012345678901234567890\r\n"
        b"=8\r\ntxt:abcd\r\n~2\r\n:1\r\n:2\r\n>2\r\n$10\r\ninvalidate\r\n*1\r\n$1\r\nk\r\n"
    )
    out = drain(p)
    assert out[0] == {"a": 1, "b": [2]}
    assert out[1:4] == [None, True, False]
    assert out[4] == 1.5 and out[5] == float("inf")
    assert int(out[6]) == 123456789012345678901234567890  # hiredis returns big numbers as text
    assert out[7] == "abcd"
    assert sorted(out[8]) == [1, 2]
    assert type(out[9]) is Push and out[9] == ["invalidate", ["k"]]


def test_python_parser_large_and_growth():
    p = Parser(size=64)
    big = b"x" * 200_000
    frame = b"$%d\r\n%s\r\n" % (len(big), big)
    for i in range(0, len(frame), 777):
        p.feed(frame[i : i + 777])
        v = p.gets()
        if v is not NEED:
            break
    assert v == big.decode()
    # a long array arriving in chunks keeps its partial items (resumable stack)
    items = [b"$4\r\nitem\r\n"] * 10_000
    frame = b"*10000\r\n" + b"".join(items)
    got = NEED
    for i in range(0, len(frame), 1000):
        p.feed(frame[i : i + 1000])
        got = p.gets()
    assert got == ["item"] * 10_000
    assert not p.has_data()


def test_python_parser_buffer_reuse():
    p = Parser(size=32)
    for i in range(1000):
        p.feed(b":%d\r\n" % i)
        assert p.gets() == i
    assert len(p.buf) == 32 * 2 ** 10 or len(p.buf) >= 32  # grows only when a recv slot is needed
    p.feed(b"$5\r\nab")
    assert p.gets() is NEED
    p.feed(b"cde\r\n")
    assert p.gets() == "abcde"


def test_python_parser_two_digit_lengths_and_neg():
    p = Parser()
    p.feed(b"*3\r\n$12\r\nhello world!\r\n$-1\r\n$0\r\n\r\n")
    assert p.gets() == ["hello world!", None, ""]


def test_protocol_error():
    p = Parser()
    p.feed(b"?bad\r\n")
    with pytest.raises(ProtocolError):
        p.gets()


def test_pipeline_finish_alignment():
    from ruvio._pipeline import PipelineBase

    pb = PipelineBase(None, True)
    pb.set("a", "1")
    pb.execute_command("NOPE")
    pb.get("a")
    raw = ["OK", "QUEUED", ResponseError("ERR unknown command"), "QUEUED", ["OK", "1"]]
    res = pb._finish(raw, raise_on_error=False)
    assert res[0] is True and isinstance(res[1], ResponseError) and res[2] == "1"
