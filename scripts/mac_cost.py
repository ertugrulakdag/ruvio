#!/usr/bin/env python3
"""Server CPU time per request, Ruvio vs Redis, on macOS/Linux (no perf or valgrind).

Each server is started fresh, loaded with the feature SETUP, driven by one pipelined
`redis-benchmark` run, then terminated; `wait4` returns its exact user+system CPU. An idle
run of the same length is subtracted. Rows are those of redis_compare_features.FEATURES.

    cargo build --profile release-fast --bin ruvio
    python3 scripts/mac_cost.py --ruvio target/release-fast/ruvio [--only SETRANGE,TOUCH]

Prints ns/request for both and the ratio Redis/Ruvio (>1 = Ruvio faster). Short on purpose.
"""

from __future__ import annotations

import argparse
import os
import signal
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import redis_compare as rc
import redis_compare_features as rf


def run_once(argv, port, setup, bench_argv, requests, pipeline, cwd):
    proc = subprocess.Popen(argv, cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    rc.wait_ping(port)

    for item in setup:
        rc.command(port, *[p.encode() for p in item])

    time.sleep(0.3)
    bench = [
        "redis-benchmark", "-h", "127.0.0.1", "-p", str(port), "-n", str(requests),
        "-c", "1", "-P", str(pipeline), "-r", "100000", "-q",
    ] + bench_argv
    # Warm the caches and the allocator first, then measure the delta of a second run.
    subprocess.run(bench, capture_output=True, check=True)
    before = cpu_of(proc.pid)
    subprocess.run(bench, capture_output=True, check=True)
    after = cpu_of(proc.pid)
    proc.send_signal(signal.SIGTERM)
    proc.wait(timeout=10)
    return (after - before) / requests * 1e9


def cpu_of(pid: int) -> float:
    out = subprocess.run(["ps", "-o", "cputime=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    # ps has 10 ms resolution; the runs are long enough (hundreds of ms) to be read this way.
    parts = out.replace(".", ":").split(":")
    hours = 0
    if len(parts) == 4:
        hours, minutes, seconds, hundredths = map(int, parts)
    else:
        minutes, seconds, hundredths = map(int, parts)
    return hours * 3600 + minutes * 60 + seconds + hundredths / 100


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ruvio", default="target/release-fast/ruvio")
    ap.add_argument("--only", default="")
    ap.add_argument("--requests", type=int, default=1_000_000)
    ap.add_argument("--pipeline", type=int, default=16)
    ap.add_argument("--rounds", type=int, default=3)
    args = ap.parse_args()
    only = [s.lower() for s in args.only.split(",") if s]
    tmp = Path(tempfile.mkdtemp(prefix="mac-cost-"))
    rport, vport = 7411, 7412
    ruvio = [args.ruvio, "--address", f"127.0.0.1:{rport}", "--durability", "memory", "--shards", "1", "--data-dir", str(tmp / "rdata")]
    redis = ["redis-server", "--bind", "127.0.0.1", "--port", str(vport), "--save", "", "--appendonly", "no", "--dir", str(tmp)]
    print(f"{'row':44} {'ruvio ns':>9} {'redis ns':>9} {'redis/ruvio':>11}")

    for label, argv, _, _ in rf.FEATURES:
        if only and not any(o in label.lower() for o in only):
            continue
        (tmp / "rw").mkdir(exist_ok=True)
        a, b = [], []
        for r in range(args.rounds):
            order = [("a", ruvio, rport), ("b", redis, vport)]
            if r % 2:
                order.reverse()
            for tag, cmd, port in order:
                v = run_once(cmd, port, rf.SETUP, argv, args.requests, args.pipeline, tmp / "rw")
                (a if tag == "a" else b).append(v)
        ra, rb = statistics.median(a), statistics.median(b)
        print(f"{label[:44]:44} {ra:9.0f} {rb:9.0f} {rb / ra:11.2f}", flush=True)


if __name__ == "__main__":
    main()
