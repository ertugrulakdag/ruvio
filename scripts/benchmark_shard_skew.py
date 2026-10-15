#!/usr/bin/env python3
"""Compare a cold shard alone and while another shard receives one hot key."""

import argparse
import csv
import io
import os
import subprocess
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from benchmark_shards import tag_for_shard
from fairness_multishard import free_port_pair, wait_ready
from memory_churn import ROOT


def bench(port, command, key, requests, clients, barrier=None):
    if barrier is not None:
        barrier.wait()
    args = ["redis-benchmark", "-h", "127.0.0.1", "-p", str(port), "--csv",
            "-n", str(requests), "-c", str(clients), "-P", "1", command, key]
    result = subprocess.run(args, capture_output=True, text=True, check=True)
    rows = list(csv.reader(io.StringIO(result.stdout)))
    row = next(row for row in reversed(rows) if len(row) >= 7 and row[1] != "rps")
    return {"rps": float(row[1]), "p50_ms": float(row[4]),
            "p95_ms": float(row[5]), "p99_ms": float(row[6])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ruvio", type=Path, default=ROOT / "target/release/ruvio")
    parser.add_argument("--requests", type=int, default=100_000)
    parser.add_argument("--clients", type=int, default=16)
    args = parser.parse_args()
    if args.requests <= 0 or args.clients <= 0:
        parser.error("requests and clients must be positive")

    port = free_port_pair()
    hot = "{" + tag_for_shard(0, 2) + "}:hot"
    cold = "{" + tag_for_shard(1, 2) + "}:cold"
    with tempfile.TemporaryDirectory(prefix="ruvio-shard-skew-") as directory:
        log_path = Path(directory) / "server.log"
        env = {key: value for key, value in os.environ.items() if not key.startswith("RUVIO_")}
        env.update(RUVIO_ADDR=f"127.0.0.1:{port}", RUVIO_SHARDS="2",
                   RUVIO_DURABILITY="memory", RUVIO_DATA_DIR=directory)
        with log_path.open("w") as log:
            server = subprocess.Popen([str(args.ruvio.resolve())], cwd=directory, env=env, stdout=log,
                                      stderr=subprocess.STDOUT)
            try:
                wait_ready(server, (port, port + 1), log_path)
                bench(port + 1, "INCR", cold, 100, 1)
                bench(port, "INCR", hot, 100, 1)
                alone = bench(port + 1, "GET", cold, args.requests, args.clients)
                barrier = threading.Barrier(2)
                with ThreadPoolExecutor(max_workers=2) as pool:
                    hot_run = pool.submit(bench, port, "INCR", hot, args.requests * 2,
                                          args.clients, barrier)
                    cold_run = pool.submit(bench, port + 1, "GET", cold, args.requests,
                                           args.clients, barrier)
                    mixed = cold_run.result()
                    hot_result = hot_run.result()
                print(f"cold shard alone GET: {alone}")
                print(f"cold shard with hot-key INCR on peer: {mixed}")
                print(f"hot-key shard INCR: {hot_result}")
                print(f"cold throughput ratio: {mixed['rps'] / alone['rps']:.3f}; "
                      f"p99 ratio: {mixed['p99_ms'] / max(alone['p99_ms'], .001):.3f}")
            finally:
                server.terminate()
                server.wait(timeout=5)


if __name__ == "__main__":
    main()
