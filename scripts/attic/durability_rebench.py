#!/usr/bin/env python3
"""Repeat the single-core durability comparison after the WAL group-commit change.

Same pins as scripts/baremetal_compare.py: server core 2, client cores 4-7.
Client is redis-benchmark. Redis is one redis-server process, not Cluster.
"""

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import time
from pathlib import Path

EV = os.environ.get("RUVIO_BIN", "/root/ruvio/target-evented/release/ruvio")
REDIS = os.environ.get("REDIS_BIN", "/usr/local/bin/redis-server-8")
OUT = Path(os.environ.get("RUVIO_OUT", "/root/ruvio/profile-out/durability-rebench"))
REQUESTS = int(os.environ.get("RUVIO_REQUESTS", "1000000"))
SAMPLES = int(os.environ.get("RUVIO_SAMPLES", "3"))
PROCS: list[subprocess.Popen] = []


def log(message: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {message}", flush=True)


def pkill() -> None:
    for proc in PROCS:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)

    PROCS.clear()
    subprocess.run(["pkill", "-x", "ruvio"], check=False)
    subprocess.run(["pkill", "-f", "redis-server-8"], check=False)
    time.sleep(0.4)


def wait_ping(port: int) -> None:
    deadline = time.perf_counter() + 15

    while time.perf_counter() < deadline:
        result = subprocess.run(["redis-cli", "-p", str(port), "PING"], capture_output=True, text=True)

        if result.stdout.strip() == "PONG":
            return

        time.sleep(0.05)

    raise TimeoutError(f"no PONG on {port}")


def start_ruvio(durability: str) -> None:
    work = Path("/var/tmp/ruvio-rebench")

    if work.exists():
        subprocess.run(["rm", "-rf", str(work)], check=True)

    work.mkdir(parents=True)
    env = os.environ.copy()
    env.update(
        {
            "RUVIO_CORE": "2",
            "RUVIO_SHARDS": "1",
            "RUVIO_CPUSET": "2",
            "RUVIO_ADDR": "127.0.0.1:6379",
            "RUVIO_ADVERTISE_HOST": "127.0.0.1",
            "RUVIO_DURABILITY": durability,
            "RUVIO_DATA_DIR": str(work / "data"),
        }
    )
    proc = subprocess.Popen(
        ["taskset", "-c", "2", EV],
        env=env,
        cwd=work,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    PROCS.append(proc)
    wait_ping(6379)


def start_redis(append_fsync: str | None) -> None:
    directory = Path("/tmp/redis-rebench")

    if directory.exists():
        subprocess.run(["rm", "-rf", str(directory)], check=True)

    directory.mkdir(parents=True)
    arguments = [
        "taskset",
        "-c",
        "2",
        REDIS,
        "--bind",
        "127.0.0.1",
        "--port",
        "6379",
        "--protected-mode",
        "no",
        "--save",
        "",
        "--dir",
        str(directory),
        "--dbfilename",
        "dump.rdb",
        "--daemonize",
        "no",
    ]

    if append_fsync is None:
        arguments.extend(["--appendonly", "no"])
    else:
        arguments.extend(["--appendonly", "yes", "--appendfsync", append_fsync])

    proc = subprocess.Popen(arguments, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    PROCS.append(proc)
    wait_ping(6379)


def parse_set(text: str) -> dict[str, float]:
    part = text.split("====== SET", 1)[-1]
    throughput = re.search(r"throughput summary: ([0-9.]+) requests per second", part)
    latency = re.search(
        r"\n\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\n",
        part,
    )
    tail = re.search(r"99\.902% <= ([0-9.]+) milliseconds", part)

    if not throughput or not latency:
        raise RuntimeError(part[:400])

    return {
        "rps": float(throughput.group(1)),
        "p50_ms": float(latency.group(3)),
        "p95_ms": float(latency.group(4)),
        "p99_ms": float(latency.group(5)),
        "p99_9_bucket_ms": float(tail.group(1)) if tail else -1,
    }


def bench() -> dict[str, float]:
    completed = subprocess.run(
        [
            "taskset",
            "-c",
            "4-7",
            "redis-benchmark",
            "-h",
            "127.0.0.1",
            "-p",
            "6379",
            "-t",
            "set",
            "-n",
            str(REQUESTS),
            "-c",
            "50",
            "-P",
            "16",
            "-d",
            "64",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    return parse_set(completed.stdout)


def median(rows: list[dict[str, float]]) -> dict[str, float]:
    import statistics

    return {key: round(statistics.median(row[key] for row in rows), 3) for key in rows[0]}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    report = {}
    cases = [
        ("evented-memory", lambda: start_ruvio("memory")),
        ("evented-everysec", lambda: start_ruvio("everysec")),
        ("evented-always", lambda: start_ruvio("always")),
        ("redis-memory", lambda: start_redis(None)),
        ("redis-everysec", lambda: start_redis("everysec")),
        ("redis-always", lambda: start_redis("always")),
    ]

    for name, start in cases:
        rows = []

        for sample in range(1, SAMPLES + 1):
            pkill()
            start()
            row = bench()
            rows.append(row)
            log(f"{name} sample {sample} {row}")
            pkill()

        report[name] = {"samples": rows, "median": median(rows)}

    (OUT / "report.json").write_text(json.dumps(report, indent=2))
    log("REBENCH_DONE")


if __name__ == "__main__":
    main()
