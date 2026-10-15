#!/usr/bin/env python3
import argparse
import binascii
import csv
import io
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor


def slot(key: str) -> int:
    return binascii.crc_hqx(key.encode(), 0) % 16384


def tag_for_shard(shard_id: int, shard_count: int) -> str:
    for candidate in range(100_000):
        tag = f"ruvio-shard-{shard_id}-{candidate}"
        if slot(tag) * shard_count // 16384 == shard_id:
            return tag
    raise RuntimeError(f"could not find hash tag for shard {shard_id}")


def run_one(
    port: int,
    command: str,
    tag: str,
    requests: int,
    clients: int,
    pipeline: int,
    data_size: int,
    barrier: threading.Barrier,
) -> dict:
    suffix = "counter:__rand_int__" if command == "INCR" else "__rand_int__"
    key = f"{{{tag}}}:{suffix}"
    arguments = [
        "redis-benchmark",
        "-h",
        "127.0.0.1",
        "-p",
        str(port),
        "--csv",
        "-n",
        str(requests),
        "-c",
        str(clients),
        "-P",
        str(pipeline),
        "-r",
        "1000000",
        "-d",
        str(data_size),
        command,
        key,
    ]
    if command == "SET":
        arguments.append("__data__")

    barrier.wait()
    completed = subprocess.run(arguments, check=True, capture_output=True, text=True)
    rows = list(csv.reader(io.StringIO(completed.stdout)))
    row = next(row for row in reversed(rows) if len(row) >= 7 and row[1] != "rps")
    return {
        "port": port,
        "command": command,
        "rps": float(row[1]),
        "avg_ms": float(row[2]),
        "p50_ms": float(row[4]),
        "p95_ms": float(row[5]),
        "p99_ms": float(row[6]),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-port", type=int, default=6379)
    parser.add_argument("--shards", type=int, required=True)
    parser.add_argument("--requests-per-shard", type=int, default=200_000)
    parser.add_argument("--clients-per-shard", type=int, default=25)
    parser.add_argument("--pipeline", type=int, default=16)
    parser.add_argument("--data-size", type=int, default=64)
    parser.add_argument("--warmup-per-shard", type=int, default=10_000)
    args = parser.parse_args()

    tags = [tag_for_shard(shard, args.shards) for shard in range(args.shards)]
    print("command,aggregate_rps,wall_seconds,max_p99_ms,shard_rps")
    for command in ("SET", "GET", "INCR"):
        warmup_barrier = threading.Barrier(args.shards)
        with ThreadPoolExecutor(max_workers=args.shards) as executor:
            list(
                executor.map(
                    lambda shard: run_one(
                        args.base_port + shard,
                        command,
                        tags[shard],
                        args.warmup_per_shard,
                        args.clients_per_shard,
                        args.pipeline,
                        args.data_size,
                        warmup_barrier,
                    ),
                    range(args.shards),
                )
            )

        barrier = threading.Barrier(args.shards)
        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.shards) as executor:
            results = list(
                executor.map(
                    lambda shard: run_one(
                        args.base_port + shard,
                        command,
                        tags[shard],
                        args.requests_per_shard,
                        args.clients_per_shard,
                        args.pipeline,
                        args.data_size,
                        barrier,
                    ),
                    range(args.shards),
                )
            )
        elapsed = time.perf_counter() - started
        aggregate = args.requests_per_shard * args.shards / elapsed
        max_p99 = max(result["p99_ms"] for result in results)
        shard_rps = ";".join(f'{result["port"]}:{result["rps"]:.2f}' for result in results)
        print(f"{command},{aggregate:.2f},{elapsed:.6f},{max_p99:.3f},{shard_rps}")


if __name__ == "__main__":
    main()