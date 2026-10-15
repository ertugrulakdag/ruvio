#!/usr/bin/env python3
"""Cost of the Bloom, JSON, IFEQ, DELEX and capped-push commands against their old equivalents.

Two modes, one server at a time, one row at a time (`redis-benchmark -P 16`):

  instructions  runs the server under callgrind and prints user instructions per request.
                Deterministic, so a noisy shared host does not matter.
  throughput    runs the plain server and prints requests per second (median of --rounds).

    python3 scripts/bench_added_commands.py --mode instructions --ruvio target/release-fast/ruvio --label new
    python3 scripts/bench_added_commands.py --mode instructions --ruvio /path/to/base/ruvio --label base
    python3 scripts/bench_added_commands.py --mode instructions --label redis --redis

Rows that a target cannot run (a command the old binary lacks, RedisBloom on plain Redis) are skipped.
Writes <out>/<label>-<mode>.json.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

import redis_compare as rc
import redis_compare_features as rf

SUMMARY = re.compile(r"^summary:\s+(.*)$", re.M)
EVENTS = re.compile(r"^events:\s+(.*)$", re.M)

ITEMS = " ".join(f"item{i}" for i in range(1000)).split()
ARRAY = "[" + ",".join(str(i) for i in range(100)) + "]"
DOC = '{"l":%s,"s":"x","n":1}' % ARRAY
LUA = (
    'if redis.call("get",KEYS[1])==ARGV[1] then return redis.call("set",KEYS[1],ARGV[2]) '
    "else return false end"
)
LUA_SHA = hashlib.sha1(LUA.encode()).hexdigest()

# (label, targets, setup frames, redis-benchmark command). `{r}` is redis-benchmark's __rand_int__.
R = "__rand_int__"
ROWS: list[tuple[str, set[str], list[tuple[str, ...]], tuple[str, ...]]] = [
    ("GET (control)", {"base", "new", "redis"}, [("SET", "g", "value")], ("GET", "g")),
    ("SET (control)", {"base", "new", "redis"}, [], ("SET", "s", "value")),
    ("LPUSH (control)", {"base", "new", "redis"}, [], ("LPUSH", "lp", "v")),
    ("BF.ADD big filter", {"base", "new"}, [("BF.RESERVE", "bf", "0.01", "1000000")], ("BF.ADD", "bf", f"item{R}")),
    ("BF.EXISTS big filter", {"base", "new"}, [("BF.RESERVE", "bfx", "0.01", "1000000"), ("BF.ADD", "bfx", "item1")], ("BF.EXISTS", "bfx", f"item{R}")),
    ("BF.MADD 10 items", {"new"}, [("BF.RESERVE", "bfm", "0.01", "1000000")], ("BF.MADD", "bfm", *[f"i{n}{R}" for n in range(10)])),
    ("BF.MEXISTS 10 items", {"new"}, [("BF.RESERVE", "bfme", "0.01", "1000000")], ("BF.MEXISTS", "bfme", *[f"i{n}{R}" for n in range(10)])),
    ("BF.ADD default filter (grows)", {"new"}, [], ("BF.ADD", "bfd", f"item{R}")),
    ("SET IFEQ match", {"new"}, [("SET", "ifk", "v")], ("SET", "ifk", "v", "IFEQ", "v")),
    ("SET IFEQ mismatch", {"new"}, [("SET", "ifm", "v")], ("SET", "ifm", "v", "IFEQ", "other")),
    ("EVALSHA compare-and-set (Redis)", {"redis"}, [("SCRIPT", "LOAD", LUA), ("SET", "ifk", "v")], ("EVALSHA", LUA_SHA, "1", "ifk", "v", "v")),
    ("DELEX IFEQ mismatch", {"new"}, [("SET", "dx", "v")], ("DELEX", "dx", "IFEQ", "other")),
    ("RPUSHCAP cap=1000", {"new"}, [("RPUSH", "cap", *ITEMS)], ("RPUSHCAP", "cap", "1000", "v")),
    ("RPUSHCAP cap=10", {"new"}, [("RPUSH", "cap10", *ITEMS[:10])], ("RPUSHCAP", "cap10", "10", "v")),
    ("RPUSH (1 of RPUSH+LTRIM)", {"base", "new", "redis"}, [("RPUSH", "pl", *ITEMS)], ("RPUSH", "pl", "v")),
    ("LTRIM keep 1000 (2 of RPUSH+LTRIM)", {"base", "new", "redis"}, [("RPUSH", "tl", *ITEMS)], ("LTRIM", "tl", "-1000", "-1")),
    ("LTRIM keep 10 (2 of RPUSH+LTRIM)", {"base", "new", "redis"}, [("RPUSH", "tl10", *ITEMS[:10])], ("LTRIM", "tl10", "-10", "-1")),
    ("JSON.GET path", {"base", "new"}, [("JSON.SET", "jd", "$", DOC)], ("JSON.GET", "jd", "$.n")),
    ("JSON.SET path", {"base", "new"}, [("JSON.SET", "jd", "$", DOC)], ("JSON.SET", "jd", "$.n", "1")),
    ("JSON.NUMMULTBY", {"new"}, [("JSON.SET", "jd", "$", DOC)], ("JSON.NUMMULTBY", "jd", "$.n", "1")),
    ("JSON.ARRTRIM 100", {"new"}, [("JSON.SET", "jd", "$", DOC)], ("JSON.ARRTRIM", "jd", "$.l", "0", "99")),
    ("JSON.ARRPOP last", {"base", "new"}, [("JSON.SET", "jd", "$", DOC)], ("JSON.ARRPOP", "jd", "$.l")),
    ("JSON.ARRINDEX", {"new"}, [("JSON.SET", "jd", "$", DOC)], ("JSON.ARRINDEX", "jd", "$.l", "99")),
    ("JSON.OBJLEN", {"new"}, [("JSON.SET", "jd", "$", DOC)], ("JSON.OBJLEN", "jd", "$")),
    ("JSON.ARRLEN (existing)", {"base", "new"}, [("JSON.SET", "jd", "$", DOC)], ("JSON.ARRLEN", "jd", "$.l")),
]


def control(pid: int, flag: str) -> None:
    subprocess.run(["callgrind_control", flag, str(pid)], capture_output=True, check=False)


def newest_dump(out: Path, seen: set[Path]) -> Path:
    for _ in range(200):
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


def server_command(args: argparse.Namespace, directory: Path, port: int) -> list[str]:
    if args.redis:
        return [args.redis_server, "--bind", "127.0.0.1", "--port", str(port), "--save", "",
                "--appendonly", "no", "--dir", str(directory)]

    return [str(args.ruvio), "--address", f"127.0.0.1:{port}", "--durability", "memory",
            "--shards", "1", "--data-dir", str(directory / "data")]


def benchmark(args: argparse.Namespace, port: int, requests: int, command: tuple[str, ...]) -> float | None:
    """redis-benchmark requests per second for one row; the output line also shows errors."""
    result = subprocess.run(
        [args.redis_benchmark, "-h", "127.0.0.1", "-p", str(port), "-n", str(requests), "-c", "10",
         "-P", "16", "-r", "100000", "-q", *command],
        capture_output=True, text=True, check=False,
    )
    match = re.search(r"([\d.]+) requests per second", result.stdout.replace("\r", "\n"))

    return float(match.group(1)) if match else None


def measure(args: argparse.Namespace) -> dict[str, float]:
    target = "redis" if args.redis else args.label
    workdir = Path(tempfile.mkdtemp(prefix=f"added-{target}-"))
    out = workdir / "cg.out"
    port = args.port
    command = server_command(args, workdir, port)

    if args.mode == "instructions":
        command = ["valgrind", "--tool=callgrind", f"--callgrind-out-file={out}", *command]

    proc = subprocess.Popen(command, cwd=workdir, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    results: dict[str, float] = {}

    try:
        rc.wait_ping(port, timeout=90)
        seen: set[Path] = set()

        for label, targets, setup, bench in ROWS:
            if (target if target in ("base", "new", "redis") else "new") not in targets:
                continue

            if args.only and not any(word.lower() in label.lower() for word in args.only):
                continue

            try:
                rf.send_all(port, [rf.frame(*frame) for frame in setup])
            except Exception as error:  # a command the target lacks
                print(f"{target:5} {label:40} skipped ({error})", flush=True)
                continue

            if args.mode == "instructions":
                requests = args.requests
                control(proc.pid, "-z")
                rate = benchmark(args, port, requests, bench)
                control(proc.pid, "-d")
                dump = newest_dump(out, seen)
                seen.add(dump)
                value = counts(dump)["Ir"] / requests
                unit = "Ir/request"
            else:
                rounds = [benchmark(args, port, args.requests, bench) for _ in range(args.rounds)]
                rounds = [r for r in rounds if r]
                value = statistics.median(rounds) if rounds else 0.0
                unit = "req/s"
                rate = value

            if not rate:
                print(f"{target:5} {label:40} no result (command rejected?)", flush=True)
                continue

            results[label] = value
            print(f"{target:5} {label:40} {value:>12.0f} {unit}", flush=True)
    finally:
        proc.terminate()
        proc.wait(timeout=60)
        shutil.rmtree(workdir, ignore_errors=True)

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mode", choices=["instructions", "throughput"], default="instructions")
    parser.add_argument("--ruvio", type=Path, default=Path("target/release-fast/ruvio"))
    parser.add_argument("--label", default="new", help="base or new (selects the rows); redis uses --redis")
    parser.add_argument("--redis", action="store_true")
    parser.add_argument("--redis-server", default=shutil.which("redis-server") or "redis-server")
    parser.add_argument("--redis-benchmark", default=shutil.which("redis-benchmark") or "redis-benchmark")
    parser.add_argument("--port", type=int, default=6461)
    parser.add_argument("--requests", type=int, default=20_000)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--only", nargs="*", help="substrings of row labels to run")
    parser.add_argument("--out", type=Path, default=Path("target/added-commands"))
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    label = "redis" if args.redis else args.label
    results = measure(args)
    (args.out / f"{label}-{args.mode}.json").write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
