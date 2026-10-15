#!/usr/bin/env python3
"""Instructions and I1 misses per request, Ruvio vs Redis, for the feature commands.

Runs each server under `valgrind --tool=callgrind --cache-sim=yes`, one at a time. For
every row of `redis_compare_features.FEATURES` it zeroes the counters, sends a small
pipelined `redis-benchmark` run and dumps. Numbers are deterministic, so unlike the
throughput table they do not depend on a noisy shared host.

    cargo build --profile release-fast --bin ruvio
    python3 scripts/command_cost.py --ruvio target/release-fast/ruvio

A few minutes in total. Writes command_cost.json and command_cost.md into --out.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import redis_compare as rc
import redis_compare_features as rf

ROOT = Path(__file__).resolve().parent.parent
SUMMARY = re.compile(r"^summary:\s+(.*)$", re.M)
EVENTS = re.compile(r"^events:\s+(.*)$", re.M)


def control(pid: int, flag: str) -> None:
    subprocess.run(["callgrind_control", flag, str(pid)], capture_output=True, check=False)


def newest_dump(out: Path, seen: set[Path]) -> Path:
    for _ in range(100):
        dumps = [p for p in out.parent.glob(out.name + ".*") if p not in seen]

        if dumps:
            time.sleep(0.2)
            return max(dumps, key=lambda p: p.stat().st_mtime)

        time.sleep(0.1)

    raise RuntimeError("callgrind dump did not appear")


def counts(path: Path) -> dict[str, int]:
    text = path.read_text(errors="replace")
    names = EVENTS.search(text).group(1).split()
    values = [int(v) for v in SUMMARY.search(text).group(1).split()]
    return dict(zip(names, values))


def measure(args: argparse.Namespace, name: str, workdir: Path) -> dict[str, dict[str, float]]:
    directory = Path(tempfile.mkdtemp(prefix=f"{name}-", dir=workdir))
    out = directory / "cg.out"
    port = args.ruvio_port if name == "ruvio" else args.redis_port

    if name == "ruvio":
        server = [str(args.ruvio), "--address", f"127.0.0.1:{port}", "--durability", "memory",
                  "--shards", "1", "--data-dir", str(directory / "data")]
    else:
        server = [args.redis_server, "--bind", "127.0.0.1", "--port", str(port), "--save", "",
                  "--appendonly", "no", "--dir", str(directory)]

    argv = ["valgrind", "--tool=callgrind", "--cache-sim=yes",
            f"--callgrind-out-file={out}", *server]
    proc = subprocess.Popen(argv, cwd=directory, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    results: dict[str, dict[str, float]] = {}

    try:
        rc.wait_ping(port, timeout=60)
        rf.send_all(port, [rf.frame(*command) for command in rf.SETUP])
        seen: set[Path] = set()

        for label, bench, _requests, _note in rf.FEATURES:
            if args.only and not any(word.lower() in label.lower() for word in args.only):
                continue

            # Ruvio is already 2x Redis here, and under valgrind one request scans every
            # GEOADD point from the row before: minutes for no information.
            if bench[0] == "GEOSEARCH" and not args.only:
                continue

            requests = args.requests // 20 if bench[0] in ("PFCOUNT", "GEOSEARCH") else args.requests
            control(proc.pid, "-z")
            subprocess.run(
                [args.redis_benchmark, "-h", "127.0.0.1", "-p", str(port), "-n", str(requests),
                 "-c", "10", "-P", "16", "-r", "100000", "-q", *bench],
                capture_output=True, check=True,
            )
            control(proc.pid, "-d")
            dump = newest_dump(out, seen)
            seen.add(dump)
            c = counts(dump)
            results[label] = {
                "instructions": c["Ir"] / requests,
                "i1_misses": c.get("I1mr", 0) / requests,
            }
            print(f"{name:5} {label:45} {results[label]['instructions']:>10.0f} Ir "
                  f"{results[label]['i1_misses']:>7.1f} I1mr", flush=True)
    finally:
        proc.terminate()
        proc.wait(timeout=30)

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ruvio", type=Path, default=ROOT / "target/release-fast/ruvio")
    parser.add_argument("--redis-server", default=shutil.which("redis-server") or "redis-server")
    parser.add_argument("--redis-benchmark", default=shutil.which("redis-benchmark") or "redis-benchmark")
    parser.add_argument("--ruvio-port", type=int, default=6451)
    parser.add_argument("--redis-port", type=int, default=6452)
    parser.add_argument("--requests", type=int, default=20_000)
    parser.add_argument("--only", nargs="*", help="substrings of row labels to run")
    parser.add_argument("--skip-redis", action="store_true", help="reuse --baseline for the Redis column")
    parser.add_argument("--baseline", type=Path, default=ROOT / "benchmark/baselines/command_cost.json")
    parser.add_argument("--out", type=Path, default=ROOT / "target/command-cost")
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    workdir = Path(tempfile.mkdtemp(prefix="command-cost-"))
    ruvio = measure(args, "ruvio", workdir)

    if args.skip_redis and args.baseline.exists():
        redis = json.loads(args.baseline.read_text())["redis"]
    else:
        redis = measure(args, "redis", workdir)

    lines = ["| Command | Ruvio Ir | Redis Ir | Ir ratio | Ruvio I1 miss | Redis I1 miss |",
             "| --- | ---: | ---: | ---: | ---: | ---: |"]

    for label, row in ruvio.items():
        other = redis.get(label)

        if not other:
            continue

        lines.append(
            f"| {label} | {row['instructions']:.0f} | {other['instructions']:.0f} | "
            f"{row['instructions'] / other['instructions']:.2f}× | {row['i1_misses']:.1f} | "
            f"{other['i1_misses']:.1f} |"
        )

    table = "\n".join(lines) + "\n"
    (args.out / "command_cost.md").write_text(table)
    (args.out / "command_cost.json").write_text(json.dumps({"ruvio": ruvio, "redis": redis}, indent=2))
    print(table)


if __name__ == "__main__":
    main()
