#!/usr/bin/env python3
"""Throughput matrix against one RESP server.

Connects to an already running Redis or Ruvio. Prints one CSV row per sample.
A later --summarize pass prints median req/s, pipeline p99, and the
Ruvio/Redis ratio. The ratio is a measurement for that row, not a product claim.

Latency is the round trip of one pipeline, matching redis-benchmark -P.
"""

from __future__ import annotations

import argparse
import csv
import socket
import statistics
import sys
import threading
import time
from pathlib import Path

from resp_conformance import encode, read_response

VALUE_64 = b"x" * 64
VALUE_1K = b"y" * 1024


def key_of(index: int) -> bytes:
    return f"k:{index % 1000}".encode()


def command_for(name: str, index: int) -> tuple[bytes, ...]:
    if name == "set":
        return (b"SET", key_of(index), VALUE_64)
    if name == "get":
        return (b"GET", key_of(index))
    if name == "incr":
        return (b"INCR", f"c:{index % 1000}".encode())
    if name == "set-1k":
        return (b"SET", f"wide:{index % 1000}".encode(), VALUE_1K)
    if name == "mset":
        frame = [b"MSET"]
        for slot in range(8):
            frame.extend((f"{{m}}:{slot}".encode(), VALUE_64))
        return tuple(frame)
    if name == "mget":
        return (b"MGET", *(f"{{m}}:{slot}".encode() for slot in range(8)))
    if name == "lpush":
        return (b"LPUSH", f"list:{index % 128}".encode(), VALUE_64)
    if name == "lpop":
        return (b"LPOP", f"pop:{index % 128}".encode())
    if name == "hset":
        return (b"HSET", f"h:{index % 256}".encode(), b"f", VALUE_64)
    if name == "hget":
        return (b"HGET", f"h:{index % 256}".encode(), b"f")
    if name == "sadd":
        return (b"SADD", f"s:{index % 256}".encode(), f"m:{index}".encode())
    if name == "zadd":
        return (b"ZADD", f"z:{index % 256}".encode(), b"1", f"m:{index}".encode())
    raise KeyError(name)


SCENARIOS = (
    ("set", 1),
    ("set", 16),
    ("get", 1),
    ("get", 16),
    ("incr", 1),
    ("incr", 16),
    ("set-1k", 16),
    ("mset", 16),
    ("mget", 16),
    ("lpush", 16),
    ("lpop", 16),
    ("hset", 16),
    ("hget", 16),
    ("sadd", 16),
    ("zadd", 16),
)


class Client:
    def __init__(self, host: str, port: int) -> None:
        self.socket = socket.create_connection((host, port), timeout=5)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.reader = self.socket.makefile("rb", buffering=0)

    def exchange(self, commands: list[tuple[bytes, ...]]) -> None:
        self.socket.sendall(b"".join(encode(*command) for command in commands))
        for command in commands:
            prefix, payload = read_response(self.reader)
            if prefix == b"-":
                raise RuntimeError(f"{command[0]!r} returned {payload!r}")

    def close(self) -> None:
        self.reader.close()
        self.socket.close()


def preload(host: str, port: int, name: str, pops: int) -> None:
    client = Client(host, port)
    try:
        if name == "get":
            client.exchange([(b"SET", key_of(index), VALUE_64) for index in range(1000)])
        elif name == "mget":
            client.exchange([command_for("mset", 0)])
        elif name == "lpop":
            commands = [
                (b"LPUSH", f"pop:{slot}".encode(), VALUE_64)
                for slot in range(128)
                for _ in range((pops // 128) + 8)
            ]
            for offset in range(0, len(commands), 64):
                client.exchange(commands[offset : offset + 64])
        elif name == "hget":
            client.exchange(
                [(b"HSET", f"h:{slot}".encode(), b"f", VALUE_64) for slot in range(256)]
            )
    finally:
        client.close()


def percentile(samples: list[float], fraction: float) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    index = min(len(ordered) - 1, max(0, round(fraction * (len(ordered) - 1))))
    return ordered[index]


def run_sample(
    host: str,
    port: int,
    name: str,
    pipeline: int,
    requests: int,
    clients: int,
    warmup: int,
) -> tuple[float, float, float]:
    per_client = requests // clients
    warmup_each = max(1, warmup // clients)
    latencies: list[float] = []
    lock = threading.Lock()
    barrier = threading.Barrier(clients + 1)
    errors: list[BaseException] = []

    def worker(worker_id: int) -> None:
        try:
            client = Client(host, port)
            try:
                for step in range(0, warmup_each, pipeline):
                    batch = [
                        command_for(name, worker_id * 1_000_000 + step + item)
                        for item in range(min(pipeline, warmup_each - step))
                    ]
                    client.exchange(batch)
                barrier.wait()
                local: list[float] = []
                for step in range(0, per_client, pipeline):
                    count = min(pipeline, per_client - step)
                    batch = [
                        command_for(name, worker_id * 1_000_000 + warmup_each + step + item)
                        for item in range(count)
                    ]
                    started = time.perf_counter()
                    client.exchange(batch)
                    local.append((time.perf_counter() - started) * 1000)
                with lock:
                    latencies.extend(local)
            finally:
                client.close()
        except BaseException as error:  # noqa: BLE001 - surfaced after join
            errors.append(error)
            try:
                barrier.abort()
            except threading.BrokenBarrierError:
                pass

    threads = [threading.Thread(target=worker, args=(worker_id,)) for worker_id in range(clients)]
    for thread in threads:
        thread.start()
    try:
        barrier.wait()
    except threading.BrokenBarrierError:
        for thread in threads:
            thread.join()
        if errors:
            raise errors[0]
        raise
    started = time.perf_counter()
    for thread in threads:
        thread.join()
    elapsed = time.perf_counter() - started
    if errors:
        raise errors[0]
    completed = per_client * clients
    return completed / elapsed, percentile(latencies, 0.50), percentile(latencies, 0.99)


def measure(args: argparse.Namespace) -> None:
    output = Path(args.out)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            ["label", "scenario", "pipeline", "sample", "rps", "p50_ms", "p99_ms", "requests", "clients"]
        )
        for name, pipeline in SCENARIOS:
            print(f"{args.label} {name} P={pipeline}", flush=True)
            preload(args.host, args.port, name, args.requests * args.samples)
            for sample in range(1, args.samples + 1):
                rps, p50, p99 = run_sample(
                    args.host,
                    args.port,
                    name,
                    pipeline,
                    args.requests,
                    args.clients,
                    args.warmup,
                )
                writer.writerow(
                    [
                        args.label,
                        name,
                        pipeline,
                        sample,
                        f"{rps:.2f}",
                        f"{p50:.3f}",
                        f"{p99:.3f}",
                        args.requests,
                        args.clients,
                    ]
                )
                handle.flush()
                print(f"  sample {sample}: {rps:.0f} req/s  p99 {p99:.3f} ms", flush=True)


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open() as handle:
        return list(csv.DictReader(handle))


def median_by(rows: list[dict[str, str]], field: str) -> dict[tuple[str, str], float]:
    grouped: dict[tuple[str, str], list[float]] = {}
    for row in rows:
        grouped.setdefault((row["scenario"], row["pipeline"]), []).append(float(row[field]))
    return {key: statistics.median(values) for key, values in grouped.items()}


def summarize(redis_csv: Path, ruvio_csv: Path) -> None:
    redis_rows = load_rows(redis_csv)
    ruvio_rows = load_rows(ruvio_csv)
    redis_rps = median_by(redis_rows, "rps")
    ruvio_rps = median_by(ruvio_rows, "rps")
    redis_p99 = median_by(redis_rows, "p99_ms")
    ruvio_p99 = median_by(ruvio_rows, "p99_ms")
    print("scenario,pipeline,redis_rps,ruvio_rps,ratio,redis_p99_ms,ruvio_p99_ms")
    for name, pipeline in SCENARIOS:
        key = (name, str(pipeline))
        left = redis_rps[key]
        right = ruvio_rps[key]
        ratio = right / left if left else 0
        print(
            f"{name},{pipeline},{left:.0f},{right:.0f},{ratio:.2f},"
            f"{redis_p99[key]:.3f},{ruvio_p99[key]:.3f}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--label", default="server")
    parser.add_argument("--out", default="")
    parser.add_argument("--requests", type=int, default=20_000)
    parser.add_argument("--clients", type=int, default=32)
    parser.add_argument("--warmup", type=int, default=2_000)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--summarize", nargs=2, metavar=("REDIS_CSV", "RUVIO_CSV"))
    args = parser.parse_args()
    if args.summarize:
        summarize(Path(args.summarize[0]), Path(args.summarize[1]))
        return
    if not args.out:
        print("--out is required unless --summarize is set", file=sys.stderr)
        sys.exit(2)
    if args.requests % args.clients != 0:
        print("--requests must be divisible by --clients", file=sys.stderr)
        sys.exit(2)
    measure(args)


if __name__ == "__main__":
    main()
