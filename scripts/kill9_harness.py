#!/usr/bin/env python3
"""Kill -9 a live Ruvio process while SET commands are in flight, then restart.

Environment: RUVIO_BIN, RUVIO_KILL_TRIALS, RUVIO_KILL_PORT, RUVIO_KILL_CPU, RUVIO_KILL_DURABILITY
(comma list), RUVIO_KILL_SEED; `RUVIO_SYNC_WORKER=on|off|auto` picks the sync path (auto = worker on Linux).

Checks that recovery starts and the keyspace is a prefix: if key N is missing,
no later key exists. `always` also requires every acknowledged key to be present.
This is a process crash, not a power loss.
"""

from __future__ import annotations

import os
import random
import shutil
import socket
import subprocess
import threading
import time
from pathlib import Path

EV = os.environ.get("RUVIO_BIN", "/root/ruvio/target-evented/release/ruvio")
TRIALS = int(os.environ.get("RUVIO_KILL_TRIALS", "12"))
PORT = int(os.environ.get("RUVIO_KILL_PORT", "6379"))
CPU = os.environ.get("RUVIO_KILL_CPU", "2")
DURABILITIES = os.environ.get("RUVIO_KILL_DURABILITY", "everysec,always").split(",")
SEED = int(os.environ.get("RUVIO_KILL_SEED", "20260928"))


def wait_ping(timeout: float = 15) -> bool:
    deadline = time.perf_counter() + timeout

    while time.perf_counter() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", PORT), timeout=0.2) as sock:
                sock.sendall(b"*1\r\n$4\r\nPING\r\n")
                data = sock.recv(32)

                if data.startswith(b"+PONG"):
                    return True
        except OSError:
            time.sleep(0.05)

    return False


def start_server(durability: str, data: Path) -> subprocess.Popen:
    env = os.environ.copy()
    env.update(
        {
            "RUVIO_CORE": CPU,
            "RUVIO_SHARDS": "1",
            "RUVIO_CPUSET": CPU,
            "RUVIO_ADDR": f"127.0.0.1:{PORT}",
            "RUVIO_ADVERTISE_HOST": "127.0.0.1",
            "RUVIO_DURABILITY": durability,
            "RUVIO_DATA_DIR": str(data),
        }
    )
    log_path = data.parent / "server.log"
    handle = log_path.open("w")

    return subprocess.Popen(
        ["taskset", "-c", CPU, EV],
        env=env,
        cwd=data.parent,
        stdout=handle,
        stderr=subprocess.STDOUT,
    )


def command(sock: socket.socket, parts: list[str]) -> bytes:
    payload = f"*{len(parts)}\r\n".encode()

    for part in parts:
        encoded = part.encode()
        payload += f"${len(encoded)}\r\n".encode() + encoded + b"\r\n"

    sock.sendall(payload)

    return read_reply(sock)


def read_reply(sock: socket.socket) -> bytes:
    chunks = b""

    while b"\r\n" not in chunks:
        piece = sock.recv(4096)

        if not piece:
            raise ConnectionError("short reply")

        chunks += piece

    return chunks


def write_until_killed(duration: float) -> tuple[int, int]:
    """Return (last acknowledged key, last sent key)."""

    sock = socket.create_connection(("127.0.0.1", PORT))
    sock.settimeout(0.2)
    sent = 0
    acked = 0
    buffer = b""
    deadline = time.perf_counter() + duration
    pipeline = 16

    try:
        while time.perf_counter() < deadline:
            batch = []

            for _ in range(pipeline):
                sent += 1
                key = f"k:{sent}"
                batch.append(f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n$1\r\n1\r\n".encode())

            sock.sendall(b"".join(batch))

            try:
                while buffer.count(b"\r\n") < pipeline:
                    piece = sock.recv(65536)

                    if not piece:
                        return acked, sent

                    buffer += piece
            except TimeoutError:
                pass

            while buffer.startswith(b"+OK\r\n"):
                buffer = buffer[5:]
                acked += 1

            if buffer.startswith(b"-"):
                raise RuntimeError(buffer[:200])
    except (ConnectionError, TimeoutError, OSError):
        pass
    finally:
        sock.close()

    return acked, sent


def exists_map(limit: int) -> list[bool]:
    present = [False] * (limit + 1)

    if limit == 0:
        return present

    sock = socket.create_connection(("127.0.0.1", PORT), timeout=5)
    sock.settimeout(5)
    cursor = 1

    while cursor <= limit:
        batch = min(200, limit - cursor + 1)
        payload = []

        for key_id in range(cursor, cursor + batch):
            key = f"k:{key_id}"
            payload.append(f"*2\r\n$6\r\nEXISTS\r\n${len(key)}\r\n{key}\r\n".encode())

        sock.sendall(b"".join(payload))
        buffer = b""
        found = 0

        while found < batch:
            piece = sock.recv(65536)

            if not piece:
                raise ConnectionError("short exists reply")

            buffer += piece

            while b"\r\n" in buffer and found < batch:
                line, buffer = buffer.split(b"\r\n", 1)
                found += 1
                present[cursor + found - 1] = line == b":1"

        cursor += batch

    sock.close()

    return present


def check_prefix(present: list[bool], acked: int, durability: str) -> str | None:
    first_missing = next((index for index in range(1, len(present)) if not present[index]), None)

    if first_missing is not None:
        later = next((index for index in range(first_missing + 1, len(present)) if present[index]), None)

        if later is not None:
            return f"hole at {first_missing} while {later} exists"

        if durability == "always" and first_missing <= acked:
            return f"acked key {first_missing} missing (acked through {acked})"

    if durability == "always" and acked > 0 and not present[acked]:
        return f"last acked key {acked} missing"

    return None


def trial(durability: str, index: int) -> None:
    root = Path(f"/var/tmp/ruvio-kill9-{PORT}/{durability}-{index}")

    if root.exists():
        shutil.rmtree(root)

    data = root / "data"
    data.mkdir(parents=True)
    proc = start_server(durability, data)

    if not wait_ping():
        raise RuntimeError(f"{durability} trial {index} did not start: {(root / 'server.log').read_text()[-400:]}")

    time.sleep(0.05)
    duration = random.uniform(0.05, 0.35)
    box: dict[str, tuple[int, int]] = {}

    def run_writer() -> None:
        box["result"] = write_until_killed(duration + 2)

    writer = threading.Thread(target=run_writer)
    writer.start()
    time.sleep(duration)
    proc.kill()
    proc.wait(timeout=5)
    writer.join(timeout=5)
    acked, sent = box.get("result", (0, 0))
    restarted = start_server(durability, data)

    try:
        if not wait_ping():
            raise RuntimeError(
                f"{durability} trial {index} did not recover: {(root / 'server.log').read_text()[-500:]}"
            )

        present = exists_map(sent)
        problem = check_prefix(present, acked, durability)

        if problem:
            raise RuntimeError(f"{durability} trial {index}: {problem} sent={sent} acked={acked}")

        kept = sum(1 for index in range(1, sent + 1) if present[index])
        print(
            f"ok {durability} trial {index} sent={sent} acked={acked} recovered={kept}",
            flush=True,
        )
    finally:
        restarted.kill()
        restarted.wait(timeout=5)


def main() -> None:
    # Only processes started here are touched: other Ruvio processes on the host stay alone.
    random.seed(SEED)

    for durability in DURABILITIES:
        for index in range(1, TRIALS + 1):
            trial(durability, index)

    print("KILL9_DONE", flush=True)


if __name__ == "__main__":
    main()
