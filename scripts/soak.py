#!/usr/bin/env python3
"""Repeat churn, pipelined commands, reconnects, and a slow client.

The default is a few seconds so a local run finishes. A 24-hour hardware soak
is still a separate release gate.
"""

from __future__ import annotations

import argparse
import socket
import time


def command(*arguments: bytes) -> bytes:
    frame = bytearray(f"*{len(arguments)}\r\n", "ascii")

    for argument in arguments:
        frame.extend(f"${len(argument)}\r\n".encode())
        frame.extend(argument)
        frame.extend(b"\r\n")

    return bytes(frame)


def read_reply(client: socket.socket) -> bytes:
    header = bytearray()

    while not header.endswith(b"\r\n"):
        chunk = client.recv(1)

        if not chunk:
            raise ConnectionError("connection closed while reading a reply")

        header.extend(chunk)

    prefix = header[:1]

    if prefix in b"+:-":
        return bytes(header)

    if prefix == b"$":
        length = int(header[1:-2])

        if length < 0:
            return bytes(header)

        body = bytearray()

        while len(body) < length + 2:
            chunk = client.recv(length + 2 - len(body))
            if not chunk:
                raise ConnectionError("connection closed while reading a bulk reply")
            body.extend(chunk)

        return bytes(header) + bytes(body)

    if prefix == b"*":
        count = int(header[1:-2])
        body = bytearray(header)

        for _ in range(max(count, 0)):
            body.extend(read_reply(client))

        return bytes(body)

    raise ValueError(f"unsupported reply {bytes(header)!r}")


def connect(host: str, port: int) -> socket.socket:
    client = socket.create_connection((host, port), timeout=2)
    client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    return client


def write_churn(client: socket.socket) -> None:
    client.sendall(command(b"SET", b"keep", b"value"))

    if read_reply(client) != b"+OK\r\n":
        raise SystemExit("SET keep failed")

    batch = bytearray()

    for index in range(64):
        key = f"k{index:02}".encode()
        batch.extend(command(b"SET", key, b"v"))
        batch.extend(command(b"DEL", key))

    client.sendall(batch)

    for _ in range(64):
        if read_reply(client) != b"+OK\r\n":
            raise SystemExit("pipelined SET failed")

        if read_reply(client) != b":1\r\n":
            raise SystemExit("pipelined DEL failed")


def one_pass(host: str, port: int) -> None:
    client = connect(host, port)
    write_churn(client)
    client.close()
    again = connect(host, port)
    again.sendall(command(b"GET", b"keep"))

    if read_reply(again) != b"$5\r\nvalue\r\n":
        raise SystemExit("value did not survive reconnect")

    slow = connect(host, port)
    slow.settimeout(0.2)
    flood = command(b"PING") * 20_000

    try:
        slow.sendall(flood)
    except (TimeoutError, socket.timeout):
        pass

    again.sendall(command(b"PING"))

    if read_reply(again) != b"+PONG\r\n":
        raise SystemExit("a slow client stalled the other connection")

    slow.close()
    again.close()


def isolated_pass(host: str, port: int, mode: str, data_client: socket.socket | None) -> None:
    if mode == "data":
        assert data_client is not None
        write_churn(data_client)
        data_client.sendall(command(b"GET", b"keep"))
        if read_reply(data_client) != b"$5\r\nvalue\r\n":
            raise SystemExit("value did not survive data churn")
    elif mode == "connections":
        for _ in range(3):
            client = connect(host, port)
            client.sendall(command(b"PING"))
            if read_reply(client) != b"+PONG\r\n":
                raise SystemExit("PING on new connection failed")
            client.close()
    elif mode == "slow":
        peer = connect(host, port)
        slow = connect(host, port)
        slow.settimeout(0.2)
        try:
            slow.sendall(command(b"PING") * 20_000)
        except (TimeoutError, socket.timeout):
            pass
        peer.sendall(command(b"PING"))
        if read_reply(peer) != b"+PONG\r\n":
            raise SystemExit("a slow client stalled the other connection")
        slow.close()
        peer.close()
    else:
        one_pass(host, port)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    duration = parser.add_mutually_exclusive_group()
    duration.add_argument("--seconds", type=float)
    duration.add_argument("--passes", type=int, help="run exactly this many workload passes")
    parser.add_argument("--pause-ms", type=float, default=0, help="pause after each pass to bound connection churn")
    parser.add_argument("--mode", choices=("full", "data", "connections", "slow"), default="full")
    args = parser.parse_args()
    if args.pause_ms < 0 or (args.seconds is not None and args.seconds <= 0):
        parser.error("--pause-ms must be nonnegative and --seconds must be positive")
    if args.passes is not None and args.passes <= 0:
        parser.error("--passes must be positive")
    seconds = 3 if args.seconds is None else args.seconds
    started = time.perf_counter()
    passes = 0

    data_client = connect(args.host, args.port) if args.mode == "data" else None
    try:
        while (passes < args.passes if args.passes is not None else time.perf_counter() - started < seconds):
            isolated_pass(args.host, args.port, args.mode, data_client)
            passes += 1
            if args.pause_ms and (args.passes is None or passes < args.passes):
                time.sleep(args.pause_ms / 1_000)
    finally:
        if data_client is not None:
            data_client.close()

    print(f"passes={passes}")
    print(f"seconds={time.perf_counter() - started:.3f}")


if __name__ == "__main__":
    main()
