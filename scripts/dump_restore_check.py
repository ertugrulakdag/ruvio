#!/usr/bin/env python3
"""DUMP/RESTORE round trip against a running Ruvio. Not comparable with Redis: the payload is ours."""

import argparse
import socket

from resp_conformance import encode, read_response


def run(host, port):
    sock = socket.create_connection((host, port))
    reader = sock.makefile("rb")

    def cmd(*args):
        sock.sendall(encode(*args))
        return read_response(reader)

    def expect(label, actual, wanted):
        if actual != wanted:
            raise AssertionError(f"{label}: got {actual!r}, wanted {wanted!r}")

    cmd(b"FLUSHALL")
    cmd(b"SET", b"s", b"hello\x00\xff")
    cmd(b"RPUSH", b"l", b"a", b"b", b"c")
    cmd(b"SADD", b"st", b"x", b"y")
    cmd(b"HSET", b"h", b"f", b"v", b"g", b"w")
    cmd(b"ZADD", b"z", b"1.5", b"a", b"2", b"b")

    for key in (b"s", b"l", b"st", b"h", b"z"):
        payload = cmd(b"DUMP", key)[1]
        copy = key + b"2"

        expect(f"RESTORE {key!r}", cmd(b"RESTORE", copy, b"0", payload), (b"+", b"OK"))
        busy = cmd(b"RESTORE", copy, b"0", payload)
        expect("BUSYKEY", busy[0] == b"-" and busy[1].startswith(b"BUSYKEY"), True)
        expect("REPLACE", cmd(b"RESTORE", copy, b"0", payload, b"REPLACE"), (b"+", b"OK"))

    expect("string", cmd(b"GET", b"s2"), (b"$", b"hello\x00\xff"))
    expect("list", cmd(b"LRANGE", b"l2", b"0", b"-1")[1], [(b"$", b"a"), (b"$", b"b"), (b"$", b"c")])
    expect("zset", cmd(b"ZRANGE", b"z2", b"0", b"-1", b"WITHSCORES")[1],
           [(b"$", b"a"), (b"$", b"1.5"), (b"$", b"b"), (b"$", b"2")])
    expect("hash", cmd(b"HGETALL", b"h2")[1], [(b"$", b"f"), (b"$", b"v"), (b"$", b"g"), (b"$", b"w")])

    payload = cmd(b"DUMP", b"s")[1]

    expect("relative ttl", cmd(b"RESTORE", b"t1", b"5000", payload), (b"+", b"OK"))
    expect("pttl set", cmd(b"PTTL", b"t1")[1] > 0, True)
    expect("ABSTTL", cmd(b"RESTORE", b"t2", b"4102444800000", payload, b"ABSTTL"), (b"+", b"OK"))
    expect("PEXPIRETIME", cmd(b"PEXPIRETIME", b"t2"), (b":", 4102444800000))
    expect("past ABSTTL", cmd(b"RESTORE", b"t3", b"1000", payload, b"ABSTTL"), (b"+", b"OK"))
    expect("past ABSTTL creates nothing", cmd(b"EXISTS", b"t3"), (b":", 0))
    expect("IDLETIME/FREQ ignored", cmd(b"RESTORE", b"t4", b"0", payload, b"IDLETIME", b"10", b"FREQ", b"5"), (b"+", b"OK"))

    flipped = bytearray(payload)
    flipped[10] ^= 1
    wrong = (b"-", b"ERR DUMP payload version or checksum are wrong")

    expect("flipped payload", cmd(b"RESTORE", b"b1", b"0", bytes(flipped)), wrong)
    expect("foreign payload", cmd(b"RESTORE", b"b1", b"0", b"junk"), wrong)
    expect("negative ttl", cmd(b"RESTORE", b"b1", b"-1", payload), (b"-", b"ERR Invalid TTL value, must be >= 0"))
    expect("missing key", cmd(b"DUMP", b"missing"), (b"$", None))
    cmd(b"XADD", b"x", b"*", b"a", b"1")
    expect("stream refused", cmd(b"DUMP", b"x"), (b"-", b"ERR DUMP is not supported for this type"))
    cmd(b"FLUSHALL")
    print("dump/restore ok")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    args = parser.parse_args()
    run(args.host, args.port)
