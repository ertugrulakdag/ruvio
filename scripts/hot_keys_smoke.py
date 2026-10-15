#!/usr/bin/env python3
"""Two-shard smoke for prefix-routed hot keys."""

import argparse
import socket
import threading

from cluster_smoke import RespError, command, encode, wait_ready


def decode_raw(port: int, payload: bytes) -> bytes:
    with socket.create_connection(("127.0.0.1", port), timeout=2) as client:
        client.sendall(payload)
        client.shutdown(socket.SHUT_WR)
        return client.recv(4096)


def main(left: int = 6379, right: int = 6380) -> None:
    wait_ready(left)
    wait_ready(right)

    assert command(left, "SET", "lock:shared", "held") == b"OK"
    assert command(right, "GET", "lock:shared") == b"held"
    assert command(right, "SET", "lock:shared", "updated") == b"OK"
    assert command(left, "GET", "lock:shared") == b"updated"
    assert command(left, "DEL", "lock:shared") == 1
    assert command(right, "GET", "lock:shared") is None

    try:
        command(left, "LPUSH", "lock:foo", "x")
    except RespError as error:
        assert error.args[0] == "ERR hot key does not support lpush", error
    else:
        raise AssertionError("LPUSH on a hot key was accepted")
    assert command(right, "GET", "lock:foo") is None

    replies = [None, None]

    def set_nx(index: int, port: int, value: str) -> None:
        replies[index] = decode_raw(port, encode("SET", "lock:global", value, "NX"))

    left_thread = threading.Thread(target=set_nx, args=(0, left, "left"))
    right_thread = threading.Thread(target=set_nx, args=(1, right, "right"))
    left_thread.start()
    right_thread.start()
    left_thread.join()
    right_thread.join()
    wins = sum(1 for reply in replies if reply == b"+OK\r\n")
    misses = sum(1 for reply in replies if reply == b"$-1\r\n")
    if wins != 1 or misses != 1:
        raise AssertionError(f"expected one NX winner and one nil, got {replies!r}")

    print("hot keys smoke test passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-port", type=int, default=6379)
    args = parser.parse_args()
    main(args.base_port, args.base_port + 1)
