#!/usr/bin/env python3
"""Writes benchmark/hot-symbols.txt, the link order of the server's hot functions.

Generic commands miss the 32 KiB instruction cache mostly through conflicts: the few
hundred cache lines one request runs are scattered over a 10 MiB binary, and lines that
share a cache set evict each other. `lld --symbol-ordering-file` (see build.rs)
places the functions listed here first and contiguously, so they cannot collide.

Each server binary given runs under callgrind while the common redis-benchmark tests and
then every row of `redis_compare_features.FEATURES` run against it, one at a time. The
functions of the first workload come first, hottest first; each later workload appends the
ones not yet listed, so the code one command adds to the shared path sits together. Symbol names carry the crate's disambiguator, which
differs per profile, so build every profile you ship and pass them all:

    cargo build --profile release-fast --bin ruvio
    cargo build --release --bin ruvio
    python3 scripts/hot_symbols.py target/release-fast/ruvio target/release/ruvio

Names that no longer exist are skipped by the linker; regenerate after larger changes to
the command path or a toolchain update. A minute or two per binary.
"""

from __future__ import annotations

import argparse
import collections
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import redis_compare as rc
import redis_compare_features as rf

ROOT = Path(__file__).resolve().parent.parent
TESTS = ("get", "set", "incr", "lpush", "rpush", "lpop", "rpop", "sadd", "hset", "spop",
         "zadd", "lrange_100", "mset")


def control(pid: int, flag: str) -> None:
    subprocess.run(["callgrind_control", flag, str(pid)], capture_output=True, check=False)


def costs_of(path: Path) -> collections.Counter:
    costs: collections.Counter = collections.Counter()
    function = None
    call_line = False

    for line in path.read_text(errors="replace").splitlines():
        if line.startswith("fn="):
            function = line[3:].strip()
        elif line.startswith("calls="):
            call_line = True
        elif line[:1].isdigit() or line[:1] in "+-":
            # The cost line after `calls=` is the callee's inclusive cost, not this function's.
            if call_line:
                call_line = False
                continue

            fields = line.split()

            if function and len(fields) > 1:
                costs[function] += int(fields[1])

    return costs


def profile(binary: Path, port: int, requests: int) -> list[collections.Counter]:
    """Per-function instruction counts, one counter per workload, common commands first."""
    directory = Path(tempfile.mkdtemp(prefix="hot-symbols-"))
    out = directory / "cg.out"
    proc = subprocess.Popen(
        ["valgrind", "--tool=callgrind", "--demangle=no", "--compress-strings=no",
         f"--callgrind-out-file={out}", str(binary), "--address", f"127.0.0.1:{port}",
         "--durability", "memory", "--shards", "1", "--data-dir", str(directory / "data")],
        cwd=directory, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    bench = ["redis-benchmark", "-p", str(port), "-n", str(requests), "-c", "10", "-P", "16",
             "-r", "100000", "-q"]
    # GEOSEARCH scans every point GEOADD left: minutes under valgrind, and it is already
    # faster than Redis.
    workloads = [["-t", test] for test in TESTS] + [
        list(command) for _label, command, _requests, _note in rf.FEATURES
        if command[0] != "GEOSEARCH"
    ]
    runs = []

    try:
        rc.wait_ping(port, timeout=60)
        rf.send_all(port, [rf.frame(*command) for command in rf.SETUP])

        for number, workload in enumerate(workloads, start=1):
            control(proc.pid, "-z")
            subprocess.run([*bench, *workload], capture_output=True, check=True)
            control(proc.pid, "-d")
            dump = Path(f"{out}.{number}")

            for _ in range(100):
                if dump.exists():
                    break

                time.sleep(0.1)

            time.sleep(0.3)
            runs.append(costs_of(dump))
    finally:
        proc.terminate()
        proc.wait(timeout=60)

    return runs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("binaries", nargs="+", type=Path)
    parser.add_argument("--port", type=int, default=6465)
    parser.add_argument("--requests", type=int, default=4000)
    parser.add_argument("--min-instructions", type=int, default=2_000,
                        help="leave out functions that ran fewer instructions in a workload")
    parser.add_argument("--out", type=Path, default=ROOT / "benchmark/hot-symbols.txt")
    args = parser.parse_args()

    names: list[str] = []
    seen: set[str] = set()

    for binary in args.binaries:
        before = len(names)

        # Each workload's functions follow the ones already placed, so what one command
        # runs beyond the shared path stays together instead of interleaved by hotness.
        for costs in profile(binary.resolve(), args.port, args.requests):
            for name, cost in costs.most_common():
                # Only symbol names the linker can place: no raw addresses or "(below main)".
                if (cost >= args.min_instructions and not name.startswith("0x")
                        and "(" not in name and name not in seen):
                    seen.add(name)
                    names.append(name)

        print(f"{binary}: {len(names) - before} symbols", file=sys.stderr)

    args.out.write_text("\n".join(names) + "\n")
    print(f"wrote {len(names)} symbols to {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
