#!/usr/bin/env python3
"""Measure single-shard request tails while large replies and a slow reader compete."""

import argparse
import json
import math
import os
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path

from memory_churn import ROOT, available_port, rss_bytes
from soak import command, connect, read_reply


VALUE = b"v" * 64
ITEMS = 2048
LIST_REPLY = f"*{ITEMS}\r\n".encode() + (b"$64\r\n" + VALUE + b"\r\n") * ITEMS


def percentiles(samples):
    ordered = sorted(samples)
    return {name: round(ordered[math.ceil(len(ordered) * fraction) - 1] / 1e6, 3)
            for name, fraction in (("p50_ms", .5), ("p95_ms", .95), ("p99_ms", .99), ("p99_9_ms", .999))}


def request(port, *args):
    with connect("127.0.0.1", port) as client:
        client.sendall(command(*args))
        return read_reply(client)


def buffer_peaks(port):
    info = request(port, b"INFO").decode("utf-8")
    fields = ("input_buffer_peak_bytes", "output_buffer_peak_bytes",
              "pubsub_queue_peak_frames", "pubsub_queue_peak_bytes")
    values = dict(line.split(":", 1) for line in info.splitlines() if ":" in line)
    return {field: int(values[field]) for field in fields}


def seed(port, small_key=b"small", list_key=b"list", large_key=b"large"):
    for _ in range(ITEMS // 32):
        reply = request(port, b"RPUSH", list_key, *([VALUE] * 32))
        if not reply.startswith(b":"):
            raise AssertionError(f"failed to seed collection: {reply!r}")
    if request(port, b"SET", small_key, b"ok") != b"+OK\r\n":
        raise AssertionError("failed to seed small key")
    if request(port, b"SET", large_key, b"x" * (128 * 1024)) != b"+OK\r\n":
        raise AssertionError("failed to seed large key")


def measure_phase(port, pid, seconds, compete, *, small_key=b"small", list_key=b"list",
                  large_key=b"large"):
    stop = threading.Event()
    errors = []
    heavy_rounds = []

    def heavy():
        try:
            with connect("127.0.0.1", port) as client:
                while not stop.is_set():
                    client.sendall(command(b"LRANGE", list_key, b"0", b"-1"))
                    response = bytearray()
                    while len(response) < len(LIST_REPLY):
                        data = client.recv(min(65536, len(LIST_REPLY) - len(response)))
                        if not data:
                            raise ConnectionError("large reply was truncated")
                        response.extend(data)
                    if response != LIST_REPLY:
                        raise AssertionError("large collection reply disagrees with seed")
                    heavy_rounds.append(1)
        except Exception as error:
            errors.append(error)
            stop.set()

    worker = threading.Thread(target=heavy) if compete else None
    slow = None
    peak = rss_bytes(pid)
    samples = []
    try:
        if compete:
            slow = connect("127.0.0.1", port)
            slow.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            slow.sendall(command(b"GET", large_key) * 256)
            worker.start()
        with connect("127.0.0.1", port) as client:
            start = time.perf_counter()
            index = 0
            while time.perf_counter() - start < seconds:
                args = (b"GET", small_key) if index % 2 == 0 else (b"SET", small_key, b"ok")
                began = time.perf_counter_ns()
                client.sendall(command(*args))
                reply = read_reply(client)
                samples.append(time.perf_counter_ns() - began)
                if reply != (b"$2\r\nok\r\n" if index % 2 == 0 else b"+OK\r\n"):
                    raise AssertionError(f"unexpected small command reply: {reply!r}")
                index += 1
                peak = max(peak, rss_bytes(pid)) if index % 25 == 0 else peak
                time.sleep(.002)
    finally:
        stop.set()
        if slow is not None:
            slow.close()
        if worker is not None:
            worker.join(timeout=5)
            if worker.is_alive():
                raise TimeoutError("collection worker failed to stop")
    if errors:
        raise errors[0]
    if not samples or (compete and not heavy_rounds):
        raise AssertionError("no useful latency or collection samples")
    return {"small_requests": len(samples), "large_replies": len(heavy_rounds),
            "peak_rss_bytes": peak, **percentiles(samples), "max_ms": round(max(samples) / 1e6, 3)}


def run(binary, seconds):
    port = available_port()
    with tempfile.TemporaryDirectory(prefix="ruvio-fairness-") as directory:
        log_path = Path(directory) / "server.log"
        env = {key: value for key, value in os.environ.items() if not key.startswith("RUVIO_")}
        env.update(RUVIO_ADDR=f"127.0.0.1:{port}", RUVIO_SHARDS="1", RUVIO_DURABILITY="memory",
                   RUVIO_DATA_DIR=directory, RUVIO_MAX_PIPELINE_COMMANDS="4",
                   RUVIO_WRITE_TIMEOUT_MS="1500")
        with log_path.open("w") as log:
            server = subprocess.Popen([str(binary)], cwd=directory, env=env,
                                      stdout=log, stderr=subprocess.STDOUT)
            try:
                for _ in range(100):
                    if server.poll() is not None:
                        raise RuntimeError(f"server exited: {log_path.read_text()}")
                    try:
                        if request(port, b"PING") == b"+PONG\r\n":
                            break
                    except (OSError, ConnectionError):
                        pass
                    time.sleep(.1)
                else:
                    raise TimeoutError("server did not start")
                seed(port)
                baseline = measure_phase(port, server.pid, seconds, False)
                baseline["buffer_peaks"] = buffer_peaks(port)
                mixed = measure_phase(port, server.pid, seconds, True)
                mixed["buffer_peaks"] = buffer_peaks(port)
                time.sleep(.2)
                logs = log_path.read_text()
                return {"baseline": baseline, "mixed": mixed,
                        "slow_reader_timed_out": "write timed out" in logs,
                        "output_buffer_limit_logged": "output buffer limit exceeded" in logs}
            finally:
                if server.poll() is None:
                    server.terminate()
                    try:
                        server.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        server.kill()
                        server.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ruvio", type=Path, default=ROOT / "target/release/ruvio")
    parser.add_argument("--seconds", type=float, default=3)
    args = parser.parse_args()
    if args.seconds <= 0:
        parser.error("--seconds must be positive")
    print(json.dumps(run(args.ruvio.resolve(), args.seconds), indent=2))


if __name__ == "__main__":
    main()
