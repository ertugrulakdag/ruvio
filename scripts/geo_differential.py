#!/usr/bin/env python3
"""Compare GEO replies with Redis on one fresh database.

Numeric bulk strings may differ in trailing digits. Those compare as floats.
Search commands in this list ask for ASC, so member order is part of the reply.
"""

from __future__ import annotations

import argparse
import socket
import time

from resp_conformance import encode, read_response


def connect(host: str, port: int):
    for attempt in range(50):
        try:
            connection = socket.create_connection((host, port), timeout=1)
            break
        except OSError:
            if attempt == 49:
                raise
            time.sleep(0.1)
    else:
        raise SystemExit(f"{host}:{port} did not accept a connection")

    return connection, connection.makefile("rwb", buffering=0)


def numeric_close(left: bytes | None, right: bytes | None) -> bool:
    if left is None or right is None:
        return left is None and right is None

    try:
        actual = float(left)
        expected = float(right)
    except ValueError:
        return False

    return abs(actual - expected) <= 1e-6 * max(1.0, abs(actual), abs(expected))


def same(actual, expected) -> bool:
    if actual[0] != expected[0]:
        return False

    left, right = actual[1], expected[1]

    if actual[0] == b"*":
        if left is None or right is None:
            return left is None and right is None

        if len(left) != len(right):
            return False

        return all(same(item, other) for item, other in zip(left, right))

    if actual[0] == b"$":
        return left == right or numeric_close(left, right)

    if actual[0] == b"-":
        if left == right:
            return True

        return left.startswith(b"ERR invalid longitude,latitude pair ") and right.startswith(
            b"ERR invalid longitude,latitude pair "
        )

    return left == right


COMMANDS = [
    (b"FLUSHDB",),
    (b"GEOADD", b"Sicily", b"13.361389", b"38.115556", b"Palermo", b"15.087269", b"37.502669", b"Catania"),
    (b"TYPE", b"Sicily"),
    (b"GEOHASH", b"Sicily", b"Palermo", b"Catania", b"missing"),
    (b"GEODIST", b"Sicily", b"Palermo", b"Catania"),
    (b"GEODIST", b"Sicily", b"Palermo", b"Catania", b"km"),
    (b"GEOPOS", b"Sicily", b"Palermo", b"Catania", b"missing"),
    (b"ZSCORE", b"Sicily", b"Palermo"),
    (b"GEOSEARCH", b"Sicily", b"FROMLONLAT", b"15", b"37", b"BYRADIUS", b"200", b"km", b"ASC"),
    (b"GEOSEARCH", b"Sicily", b"FROMMEMBER", b"Palermo", b"BYRADIUS", b"100", b"km", b"ASC"),
    (
        b"GEOSEARCH",
        b"Sicily",
        b"FROMLONLAT",
        b"15",
        b"37",
        b"BYRADIUS",
        b"200",
        b"km",
        b"ASC",
        b"WITHDIST",
        b"WITHCOORD",
        b"WITHHASH",
    ),
    (b"GEOSEARCH", b"Sicily", b"FROMLONLAT", b"15", b"37", b"BYBOX", b"400", b"400", b"km", b"ASC"),
    (b"GEOSEARCH", b"Sicily", b"FROMLONLAT", b"15", b"37", b"BYRADIUS", b"200", b"km", b"COUNT", b"1", b"ASC"),
    (b"GEOSEARCHSTORE", b"near", b"Sicily", b"FROMMEMBER", b"Palermo", b"BYRADIUS", b"100", b"km"),
    (b"ZRANGE", b"near", b"0", b"-1"),
    (b"GEORADIUS", b"Sicily", b"15", b"37", b"200", b"km", b"ASC"),
    (b"GEORADIUSBYMEMBER", b"Sicily", b"Palermo", b"100", b"km"),
    (b"GEORADIUS", b"Sicily", b"15", b"37", b"200", b"km", b"STORE", b"stored"),
    (b"ZCARD", b"stored"),
    (b"GEORADIUS", b"Sicily", b"15", b"37", b"200", b"km", b"STOREDIST", b"distances"),
    (b"ZRANGE", b"distances", b"0", b"-1", b"WITHSCORES"),
    (b"GEOADD", b"Sicily", b"NX", b"13.361389", b"38.115556", b"Palermo"),
    (b"GEOADD", b"Sicily", b"XX", b"CH", b"13.361000", b"38.115000", b"Palermo"),
    (b"GEOADD", b"Sicily", b"200", b"10", b"bad"),
    (b"SET", b"word", b"value"),
    (b"GEOADD", b"word", b"13", b"38", b"nope"),
    (b"GEODIST", b"missing-key", b"a", b"b"),
    (b"GEOSEARCH", b"missing-key", b"FROMLONLAT", b"15", b"37", b"BYRADIUS", b"1", b"km"),
]


def run(host: str, ruvio_port: int, redis_port: int) -> None:
    ruvio_connection, ruvio = connect(host, ruvio_port)
    redis_connection, redis = connect(host, redis_port)

    try:
        for command in COMMANDS:
            frame = encode(*command)
            ruvio.write(frame)
            redis.write(frame)
            ruvio.flush()
            redis.flush()
            actual = read_response(ruvio)
            expected = read_response(redis)

            if not same(actual, expected):
                raise SystemExit(
                    f"{command!r} differs\n  Ruvio={actual!r}\n  Redis={expected!r}"
                )

            print("ok", command[0].decode(), flush=True)
    finally:
        ruvio_connection.close()
        redis_connection.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--ruvio-port", type=int, default=6394)
    parser.add_argument("--redis-port", type=int, default=6398)
    args = parser.parse_args()
    run(args.host, args.ruvio_port, args.redis_port)
    print("geo differential ok", flush=True)


if __name__ == "__main__":
    main()
