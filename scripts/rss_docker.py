#!/usr/bin/env python3
"""Load 1 million 64-byte strings into a fresh Ruvio container and print RSS.

The container name is ruvio-rss, never the existing ruvio or redis containers.
Each run deletes that container and its volume so the keyspace starts empty.
"""

from __future__ import annotations

import argparse
import socket
import subprocess
import time

KEYS = 1_000_000
VALUE = b"x" * 64
PIPELINE = 64
CONTAINER = "ruvio-rss"
REDIS_CONTAINER = "redis-rss"


def docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def reset_container(image: str, port: int, container: str, redis: bool) -> None:
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

    raise SystemExit("server did not answer PING")


def load(port: int) -> float:
    sock = socket.create_connection(("127.0.0.1", port))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    started = time.perf_counter()
    pending = 0
    sent = 0

    def encode(index: int) -> bytes:
        key = f"k:{index}".encode()

        return (
            b"*3\r\n$3\r\nSET\r\n$"
            + str(len(key)).encode()
            + b"\r\n"
            + key
            + b"\r\n$64\r\n"
            + VALUE
            + b"\r\n"
        )

    while sent < KEYS:
        batch = min(PIPELINE, KEYS - sent)
        sock.sendall(b"".join(encode(sent + offset) for offset in range(batch)))
        sent += batch
        pending += batch

        if pending >= PIPELINE * 8 or sent == KEYS:
            while pending:
                chunk = sock.recv(65536)

                if not chunk:
                    raise SystemExit(f"connection closed after {sent - pending} replies")

                pending -= chunk.count(b"+OK\r\n")

                if chunk.startswith(b"-") or b"\r\n-" in chunk:
                    raise SystemExit(chunk[:200])

    sock.close()

    return time.perf_counter() - started


def dbsize(port: int) -> int:
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        sock.sendall(b"*1\r\n$6\r\nDBSIZE\r\n")
        line = sock.recv(64)

    return int(line.split(b"\r\n", 1)[0][1:])


def rss(container: str) -> dict[str, str]:
    status = docker("exec", container, "cat", "/proc/1/status").stdout
    fields = {}

    for line in status.splitlines():
        if line.startswith(("VmRSS:", "VmHWM:", "VmSize:")):
            name, value = line.split(":", 1)
            fields[name] = value.strip()

    stats = docker("stats", "--no-stream", "--format", "{{.MemUsage}}", container).stdout.strip()
    fields["docker"] = stats

    return fields


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", default="ruvio-rss:probe")
    parser.add_argument("--port", type=int, default=6399)
    parser.add_argument("--label", default="probe")
    parser.add_argument("--redis", action="store_true")
    args = parser.parse_args()
    container = REDIS_CONTAINER if args.redis else CONTAINER
    reset_container(args.image, args.port, container, args.redis)
    wait_pong(args.port)
    seconds = load(args.port)
    keys = dbsize(args.port)
    memory = rss(container)
    print(f"label={args.label} keys={keys} load_s={seconds:.1f} {memory}", flush=True)


if __name__ == "__main__":
    main()
