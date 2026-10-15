#!/usr/bin/env python3
"""Mixed SET/GET/INCR/DEL/TTL probe against a fresh Ruvio or Redis container.

Does not use the existing ruvio or redis containers. Prints throughput, DBSIZE,
and VmRSS after the same operation mix.
"""

from __future__ import annotations

import argparse
import socket
import subprocess
import time

OPERATIONS = 200_000
KEYS = 50_000
PIPELINE = 64
SIZES = (16, 64, 256, 2048)


def docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def command(*arguments: bytes) -> bytes:
    frame = bytearray(f"*{len(arguments)}\r\n".encode())

    for argument in arguments:
        frame.extend(f"${len(argument)}\r\n".encode())
        frame.extend(argument)
        frame.extend(b"\r\n")

    return bytes(frame)


def operation(index: int, values: dict[int, bytes], profile: str) -> bytes:
    key = f"k:{index % KEYS}".encode()
    kind = index % 10

    if profile == "hot":
        kind = kind % 7

    if kind in (0, 1):
        size = SIZES[(index // 10) % len(SIZES)]

        return command(b"SET", key, values[size])

    if kind in (2, 3, 4):
        return command(b"GET", key)

    if kind in (5, 6):
        return command(b"INCR", f"c:{index % KEYS}".encode())

    if kind == 7:
        return command(b"DEL", key)

    return command(b"SET", key, values[64], b"PX", b"30000")


def consume(buffer: bytearray, pending: int) -> int:
    offset = 0

    while pending and offset < len(buffer):
        kind = buffer[offset]

        if kind in (ord("+"), ord("-"), ord(":")):
            end = buffer.find(b"\r\n", offset)

            if end < 0:
                break

            offset = end + 2
            pending -= 1

            continue

        if kind != ord("$"):
            raise SystemExit(bytes(buffer[offset : offset + 80]))

        line_end = buffer.find(b"\r\n", offset)

        if line_end < 0:
            break

        length = int(buffer[offset + 1 : line_end])
        start = line_end + 2

        if length < 0:
            offset = start
            pending -= 1

            continue

        if len(buffer) < start + length + 2:
            break

        offset = start + length + 2
        pending -= 1

    del buffer[:offset]

    return pending


def run_mix(port: int, profile: str) -> float:
    values = {size: b"x" * size for size in SIZES}
    sock = socket.create_connection(("127.0.0.1", port))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    pending = 0
    sent = 0
    buffer = bytearray()
    started = time.perf_counter()

    while sent < OPERATIONS:
        batch = min(PIPELINE, OPERATIONS - sent)
        sock.sendall(
            b"".join(operation(sent + offset, values, profile) for offset in range(batch))
        )
        sent += batch
        pending += batch

        if pending >= PIPELINE * 4 or sent == OPERATIONS:
            while pending:
                chunk = sock.recv(1 << 16)

                if not chunk:
                    raise SystemExit(f"connection closed with {pending} replies left")

                buffer.extend(chunk)
                pending = consume(buffer, pending)

    sock.close()

    return time.perf_counter() - started


def dbsize(port: int) -> int:
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        sock.sendall(b"*1\r\n$6\r\nDBSIZE\r\n")
        line = sock.recv(64)

    return int(line.split(b"\r\n", 1)[0][1:])


def rss(container: str) -> str:
    status = docker("exec", container, "cat", "/proc/1/status").stdout

    return next(
        line.split(":", 1)[1].strip()
        for line in status.splitlines()
        if line.startswith("VmRSS:")
    )


def wait_pong(port: int) -> None:
    deadline = time.perf_counter() + 30

    while time.perf_counter() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5) as sock:
                sock.sendall(b"*1\r\n$4\r\nPING\r\n")

                if sock.recv(16).startswith(b"+PONG"):
                    return
        except OSError:
            time.sleep(0.1)

    raise SystemExit(f"no pong on {port}")


def reset(container: str, port: int, redis: bool, image: str) -> None:
    docker("rm", "-f", container, check=False)

    if redis:
        docker(
            "run",
            "-d",
            "--name",
            container,
            "-p",
            f"127.0.0.1:{port}:6379",
            image,
            "redis-server",
            "--save",
            "",
            "--appendonly",
            "no",
            "--protected-mode",
            "no",
        )

        return

    docker(
        "run",
        "-d",
        "--name",
        container,
        "-p",
        f"127.0.0.1:{port}:6379",
        "-e",
        "RUVIO_DURABILITY=memory",
        "-e",
        "RUVIO_SHARDS=1",
        "-e",
        "RUVIO_ALLOW_UNAUTHENTICATED=true",
        "-e",
        "RUVIO_PROTECTED_MODE=false",
        image,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ruvio-image", default="salihcantekin/ruvio:0.2.5")
    parser.add_argument("--redis-image", default="redis:8-alpine")
    parser.add_argument("--profile", choices=("full", "hot"), default="full")
    args = parser.parse_args()

    for name, port, redis, image in (
        ("ruvio", 6399, False, args.ruvio_image),
        ("redis", 6398, True, args.redis_image),
    ):
        container = "ruvio-rss" if name == "ruvio" else "redis-rss"
        reset(container, port, redis, image)
        wait_pong(port)
        seconds = run_mix(port, args.profile)
        keys = dbsize(port)
        memory = rss(container)
        print(
            f"profile={args.profile} server={name} ops={OPERATIONS} keys={keys} "
            f"load_s={seconds:.2f} ops_per_s={OPERATIONS / seconds:.0f} VmRSS={memory}",
            flush=True,
        )
        docker("rm", "-f", container, check=False)


if __name__ == "__main__":
    main()
