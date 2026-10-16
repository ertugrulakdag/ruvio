#!/usr/bin/env python3
"""Bare-metal Ruvio vs Redis on one host. Same client, disjoint CPU pins.

This is the 2026-09-28 Cherry EPYC runner. Paths are absolute on that host
(`/root/ruvio`, `/usr/local/bin/redis-server-8`). See docs/evidence/2026-09-28-bare-metal.md.
"""

from __future__ import annotations

import json
import os
import re
import signal
import statistics
import subprocess
import time
from pathlib import Path

OUT = Path("/root/ruvio/profile-out/compare-20260928")
EV = "/root/ruvio/target-evented/release/ruvio"
IO = "/root/ruvio/target-iouring/release/ruvio"
REDIS = "/usr/local/bin/redis-server-8"
SHARD_BENCH = "/root/ruvio/scripts/benchmark_shards.py"
PROCS: list[subprocess.Popen] = []


def log(message: str) -> None:
    line = f"{time.strftime('%H:%M:%S')} {message}"
    print(line, flush=True)
    with (OUT / "progress.log").open("a") as handle:
        handle.write(line + "\n")


def pkill() -> None:
    for proc in PROCS:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)

    PROCS.clear()
    subprocess.run(["pkill", "-x", "ruvio"], check=False)
    subprocess.run(["pkill", "-f", "redis-server-8"], check=False)
    time.sleep(0.4)


def wait_ping(port: int, timeout: float = 15) -> None:
    deadline = time.perf_counter() + timeout

    while time.perf_counter() < deadline:
        result = subprocess.run(
            ["redis-cli", "-p", str(port), "PING"],
            capture_output=True,
            text=True,
        )

        if result.stdout.strip() == "PONG":
            return

        time.sleep(0.05)

    raise TimeoutError(f"no PONG on {port}")


def start_ruvio(binary: str, cores: str, shards: int, port: int, durability: str, log_path: Path) -> int:
    env = os.environ.copy()
    env.update(
        {
            "RUVIO_CORE": cores.split("-")[0].split(",")[0],
            "RUVIO_SHARDS": str(shards),
            "RUVIO_CPUSET": cores,
            "RUVIO_ADDR": f"127.0.0.1:{port}",
            "RUVIO_ADVERTISE_HOST": "127.0.0.1",
            "RUVIO_DURABILITY": durability,
        }
    )
    log_path.parent.mkdir(parents=True, exist_ok=True)
    work = Path("/var/tmp/ruvio-run")
    work.mkdir(parents=True, exist_ok=True)
    handle = log_path.open("w")
    proc = subprocess.Popen(
        ["taskset", "-c", cores, binary],
        env=env,
        cwd=work,
        stdout=handle,
        stderr=subprocess.STDOUT,
    )
    PROCS.append(proc)

    for offset in range(shards):
        wait_ping(port + offset)

    return proc.pid


def start_redis(core: str, port: int, append_fsync: str | None, log_path: Path) -> int:
    directory = Path(f"/tmp/redis-{port}")
    directory.mkdir(parents=True, exist_ok=True)
    arguments = [
        "taskset",
        "-c",
        core,
        REDIS,
        "--bind",
        "127.0.0.1",
        "--port",
        str(port),
        "--protected-mode",
        "no",
        "--save",
        "",
        "--dir",
        str(directory),
        "--dbfilename",
        "dump.rdb",
        "--logfile",
        str(log_path),
        "--daemonize",
        "no",
    ]

    if append_fsync is None:
        arguments.extend(["--appendonly", "no"])
    else:
        arguments.extend(["--appendonly", "yes", "--appendfsync", append_fsync])

    log_path.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(arguments, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    PROCS.append(proc)
    wait_ping(port)

    return proc.pid


def rss_kib(pid: int) -> int:
    status = Path(f"/proc/{pid}/status")

    if not status.exists():
        return 0

    for line in status.read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])

    return 0


def engine_rss_kib() -> int:
    total = 0

    for status in Path("/proc").glob("*/status"):
        try:
            text = status.read_text()
        except OSError:
            continue

        if "Name:\truvio\n" in text or "Name:\tredis-server-8\n" in text or "Name:\tredis-server\n" in text:
            for line in text.splitlines():
                if line.startswith("VmRSS:"):
                    total += int(line.split()[1])

    return total


def parse_benchmark(text: str) -> dict[str, dict[str, float]]:
    found: dict[str, dict[str, float]] = {}

    for part in text.split("====== "):
        command = part.split(" ", 1)[0].strip()

        if command not in {"SET", "GET", "INCR"}:
            continue

        throughput = re.search(r"throughput summary: ([0-9.]+) requests per second", part)
        latency = re.search(
            r"\n\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\n",
            part,
        )
        tail = re.search(r"99\.902% <= ([0-9.]+) milliseconds", part)

        if not throughput or not latency:
            continue

        found[command] = {
            "rps": float(throughput.group(1)),
            "p50_ms": float(latency.group(3)),
            "p95_ms": float(latency.group(4)),
            "p99_ms": float(latency.group(5)),
            "p99_9_bucket_ms": float(tail.group(1)) if tail else -1,
        }

    return found


def bench_ports(ports: list[int], client_cores: str, requests: int, clients: int, pipeline: int, data_size: int) -> dict:
    if len(ports) == 1:
        completed = subprocess.run(
            [
                "taskset",
                "-c",
                client_cores,
                "redis-benchmark",
                "-h",
                "127.0.0.1",
                "-p",
                str(ports[0]),
                "-t",
                "set,get,incr",
                "-n",
                str(requests),
                "-c",
                str(clients),
                "-P",
                str(pipeline),
                "-d",
                str(data_size),
            ],
            check=True,
            capture_output=True,
            text=True,
        )

        return {"kind": "single", "commands": parse_benchmark(completed.stdout), "raw": completed.stdout}

    completed = subprocess.run(
        [
            "taskset",
            "-c",
            client_cores,
            "python3",
            SHARD_BENCH,
            "--base-port",
            str(ports[0]),
            "--shards",
            str(len(ports)),
            "--requests-per-shard",
            str(requests),
            "--clients-per-shard",
            str(clients),
            "--pipeline",
            str(pipeline),
            "--data-size",
            str(data_size),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    commands = {}

    for line in completed.stdout.splitlines():
        if line.startswith(("SET,", "GET,", "INCR,")):
            name, rps, _wall, p99, _shards = line.split(",", 4)
            commands[name] = {
                "rps": float(rps),
                "p50_ms": -1,
                "p95_ms": -1,
                "p99_ms": float(p99),
                "p99_9_bucket_ms": -1,
            }

    return {"kind": "quad", "commands": commands, "raw": completed.stdout}


def median(values: list[float]) -> float:
    return statistics.median(values)


def run_case(name: str, start, ports: list[int], client_cores: str, requests: int, clients: int, pipeline: int, data_size: int, samples: int) -> dict:
    rows = []

    for sample in range(1, samples + 1):
        pkill()
        directory = OUT / name / f"sample-{sample}"
        directory.mkdir(parents=True, exist_ok=True)
        pid = start(directory / "server.log")
        result = bench_ports(ports, client_cores, requests, clients, pipeline, data_size)
        (directory / "benchmark.txt").write_text(result["raw"])
        identity = subprocess.run(
            ["redis-cli", "-p", str(ports[0]), "INFO"],
            capture_output=True,
            text=True,
        ).stdout
        (directory / "info.txt").write_text(identity)
        backend = "redis"

        for line in identity.splitlines():
            if line.startswith("ruvio_backend:") or line.startswith("redis_version:"):
                backend = line.strip()

        rows.append({"sample": sample, "pid": pid, "rss_kib": engine_rss_kib(), "backend": backend, "commands": result["commands"]})
        log(f"{name} sample {sample} {result['commands'].get('SET', {})}")

    summary = {"samples": rows, "median": {}}

    for command in ("SET", "GET", "INCR"):
        present = [row["commands"][command] for row in rows if command in row["commands"]]

        if not present:
            continue

        summary["median"][command] = {
            key: round(median([item[key] for item in present]), 3)
            for key in ("rps", "p50_ms", "p95_ms", "p99_ms", "p99_9_bucket_ms")
        }

    (OUT / name / "summary.json").write_text(json.dumps(summary, indent=2))
    pkill()

    return summary["median"]


def soak(label: str, start, ports: list[int], client_cores: str, seconds: int) -> None:
    pkill()
    directory = OUT / "soak" / label
    directory.mkdir(parents=True, exist_ok=True)
    pid = start(directory / "server.log")
    deadline = time.perf_counter() + seconds
    series = []

    while time.perf_counter() < deadline:
        started = time.perf_counter()
        result = bench_ports(ports, client_cores, 400000, 50 if len(ports) == 1 else 25, 16, 64)
        series.append(
            {
                "t": round(time.time(), 1),
                "rss_kib": engine_rss_kib(),
                "commands": result["commands"],
                "elapsed": round(time.perf_counter() - started, 3),
            }
        )
        log(f"soak {label} rss={series[-1]['rss_kib']} set={result['commands'].get('SET', {}).get('rps')}")

    (directory / "series.json").write_text(json.dumps(series, indent=2))
    pkill()


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "host.txt").write_text(
        subprocess.run(["bash", "-lc", "uname -a; lscpu | sed -n '1,25p'; echo smt=$(cat /sys/devices/system/cpu/smt/control); echo gov=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)"], capture_output=True, text=True).stdout
        + "\n"
        + subprocess.run([REDIS, "--version"], capture_output=True, text=True).stdout
    )
    report: dict = {"matrix": {}, "sweeps": {}, "durability": {}}
    single_requests = 2_000_000
    quad_requests = 500_000

    def evented_one(path: Path) -> int:
        return start_ruvio(EV, "2", 1, 6379, "memory", path)

    def iouring_one(path: Path) -> int:
        return start_ruvio(IO, "2", 1, 6379, "memory", path)

    def redis_one(path: Path) -> int:
        return start_redis("2", 6379, None, path)

    def evented_four(path: Path) -> int:
        return start_ruvio(EV, "2-5", 4, 6379, "memory", path)

    def iouring_four(path: Path) -> int:
        return start_ruvio(IO, "2-5", 4, 6379, "memory", path)

    def redis_four(path: Path) -> int:
        first = start_redis("2", 6379, None, path)

        for index, core in enumerate(("3", "4", "5"), start=1):
            start_redis(core, 6379 + index, None, path.with_name(f"server-{6379 + index}.log"))

        return first

    cases = [
        ("single-evented", evented_one, [6379], "4-7", single_requests, 50, 5),
        ("single-iouring", iouring_one, [6379], "4-7", single_requests, 50, 5),
        ("single-redis", redis_one, [6379], "4-7", single_requests, 50, 5),
        ("quad-evented", evented_four, [6379, 6380, 6381, 6382], "8-11", quad_requests, 25, 5),
        ("quad-iouring", iouring_four, [6379, 6380, 6381, 6382], "8-11", quad_requests, 25, 5),
        ("quad-redis", redis_four, [6379, 6380, 6381, 6382], "8-11", quad_requests, 25, 5),
    ]

    for name, start, ports, clients_cores, requests, clients, samples in cases:
        try:
            report["matrix"][name] = run_case(name, start, ports, clients_cores, requests, clients, 16, 64, samples)
        except Exception as error:
            log(f"FAIL {name} {error}")
            report["matrix"][name] = {"error": str(error)}
            pkill()

    for pipeline in (1, 16, 64):
        for label, start in (("evented", evented_one), ("redis", redis_one)):
            key = f"{label}-pipeline-{pipeline}"

            try:
                report["sweeps"][key] = run_case(key, start, [6379], "4-7", 1_000_000, 50, pipeline, 64, 1)
            except Exception as error:
                log(f"FAIL {key} {error}")
                report["sweeps"][key] = {"error": str(error)}
                pkill()

    for clients in (1, 50, 200):
        for label, start in (("evented", evented_one), ("redis", redis_one)):
            key = f"{label}-clients-{clients}"

            try:
                report["sweeps"][key] = run_case(key, start, [6379], "4-7", 1_000_000, clients, 16, 64, 1)
            except Exception as error:
                log(f"FAIL {key} {error}")
                report["sweeps"][key] = {"error": str(error)}
                pkill()

    for size in (64, 1024, 4096):
        for label, start in (("evented", evented_one), ("redis", redis_one)):
            key = f"{label}-bytes-{size}"

            try:
                report["sweeps"][key] = run_case(key, start, [6379], "4-7", 1_000_000, 50, 16, size, 1)
            except Exception as error:
                log(f"FAIL {key} {error}")
                report["sweeps"][key] = {"error": str(error)}
                pkill()

    def evented_everysec(path: Path) -> int:
        return start_ruvio(EV, "2", 1, 6379, "everysec", path)

    def evented_always(path: Path) -> int:
        return start_ruvio(EV, "2", 1, 6379, "always", path)

    def redis_everysec(path: Path) -> int:
        return start_redis("2", 6379, "everysec", path)

    for name, start in (
        ("durability-evented-memory", evented_one),
        ("durability-evented-everysec", evented_everysec),
        ("durability-evented-always", evented_always),
        ("durability-redis-everysec", redis_everysec),
    ):
        try:
            report["durability"][name] = run_case(name, start, [6379], "4-7", 1_000_000, 50, 16, 64, 1)
        except Exception as error:
            log(f"FAIL {name} {error}")
            report["durability"][name] = {"error": str(error)}
            pkill()

    for label, start, ports, cores in (
        ("evented-quad", evented_four, [6379, 6380, 6381, 6382], "8-11"),
        ("redis-quad", redis_four, [6379, 6380, 6381, 6382], "8-11"),
    ):
        try:
            soak(label, start, ports, cores, 15 * 60)
        except Exception as error:
            log(f"FAIL soak {label} {error}")
            pkill()

    (OUT / "report.json").write_text(json.dumps(report, indent=2))
    log("SUITE_DONE")


if __name__ == "__main__":
    main()
