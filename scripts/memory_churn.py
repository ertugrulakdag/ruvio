#!/usr/bin/env python3
"""Compare process RSS and reported memory under the same paced RESP churn."""

import argparse
import json
import os
import re
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def available_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def rss_bytes(pid):
    return int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True).strip()) * 1024


def command(port, *args):
    return subprocess.check_output(
        ["redis-cli", "-h", "127.0.0.1", "-p", str(port), "--raw", *args],
        text=True,
        stderr=subprocess.DEVNULL,
    )


def memory_info(port):
    fields = {}
    for line in command(port, "INFO", "memory").splitlines():
        if ":" in line:
            key, value = line.strip().split(":", 1)
            if key in ("charged_memory_bytes", "key_memory_bytes", "value_memory_bytes",
                       "collection_memory_bytes", "index_memory_bytes", "pooled_value_bytes",
                       "used_memory", "used_memory_rss"):
                fields[key] = int(value)
    return fields


def vmmap_summary(pid):
    summary = subprocess.check_output(["vmmap", "-summary", str(pid)], text=True)

    def bytes_from_vmmap(value):
        size = re.fullmatch(r"([0-9.]+)([KMG])", value)
        return round(float(size[1]) * {"K": 1024, "M": 1024**2, "G": 1024**3}[size[2]]) if size else None

    regions = {}
    footprint = None
    for line in summary.splitlines():
        if line.startswith("Physical footprint:"):
            footprint = bytes_from_vmmap(line.split(":", 1)[1].strip())
        if not line.startswith(("MALLOC", "STACK", "Stack", "VM_ALLOCATE")) or line.startswith("MALLOC ZONE"):
            continue
        fields = re.split(r"\s{2,}", line.strip())
        if len(fields) >= 2 and (resident := bytes_from_vmmap(fields[1])) is not None:
            regions[fields[0]] = resident
    return {"physical_footprint_bytes": footprint, "resident_regions_bytes": regions}


def pipeline(port, commands, batch=10_000):
    """Send RESP commands in batches; every reply is one line (+OK, :n)."""
    with socket.create_connection(("127.0.0.1", port)) as client:
        for start in range(0, len(commands), batch):
            chunk = commands[start:start + batch]
            client.sendall(b"".join(
                b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n%s\r\n" % (len(part), part) for part in parts)
                for parts in chunk))
            pending, buffer = len(chunk), b""
            while pending:
                buffer += client.recv(1 << 20)
                lines = buffer.split(b"\r\n")
                buffer = lines.pop()
                pending -= len(lines)


def log_fields(port):
    fields = {}
    for line in command(port, "INFO").splitlines():
        if line.startswith("log_") and ":" in line:
            key, value = line.strip().split(":", 1)
            fields[key] = int(value)
    return fields


def measure_delete_half(name, binary, keys, value_size, settle_seconds, vmmap=False):
    """Load `keys` strings, delete a random half, and compare RSS before and after."""
    import random

    port = available_port()
    with tempfile.TemporaryDirectory(prefix=f"ruvio-memory-{name}-") as directory:
        with open(Path(directory) / "server.log", "w") as log:
            server = start_server(name, binary, port, directory, log)
            try:
                baseline = rss_bytes(server.pid)
                value = b"v" * value_size
                pipeline(port, [(b"SET", b"key:%08d" % index, value) for index in range(keys)])
                time.sleep(1)
                loaded = {"rss_bytes": rss_bytes(server.pid), "info": memory_info(port)}
                if vmmap:
                    loaded["footprint_bytes"] = vmmap_summary(server.pid)["physical_footprint_bytes"]
                victims = random.Random(7).sample(range(keys), keys // 2)
                pipeline(port, [(b"DEL", b"key:%08d" % index) for index in victims])
                started = time.monotonic()
                previous = None
                while time.monotonic() - started < settle_seconds:
                    time.sleep(0.5)
                    current = log_fields(port) if name == "ruvio" else None
                    if current is not None and current == previous:
                        break
                    previous = current
                deleted = {"rss_bytes": rss_bytes(server.pid), "info": memory_info(port),
                           "settle_seconds": round(time.monotonic() - started, 2)}
                if name == "ruvio":
                    deleted["logs"] = log_fields(port)
                if vmmap:
                    deleted["footprint_bytes"] = vmmap_summary(server.pid)["physical_footprint_bytes"]
                live_drop = loaded["rss_bytes"] - baseline
                return {
                    "engine": name, "keys": keys, "value_size": value_size,
                    "baseline_rss_bytes": baseline, "loaded": loaded, "after_delete": deleted,
                    "rss_drop_bytes": loaded["rss_bytes"] - deleted["rss_bytes"],
                    "rss_drop_vs_half_data": round((loaded["rss_bytes"] - deleted["rss_bytes"]) / max(1, live_drop / 2), 3),
                }
            finally:
                stop_server(server)


def start_server(name, binary, port, directory, log):
    if name == "ruvio":
        env = {
            **{key: value for key, value in os.environ.items() if not key.startswith("RUVIO_")},
            "RUVIO_ADDR": f"127.0.0.1:{port}",
            "RUVIO_SHARDS": "1",
            "RUVIO_DURABILITY": "memory",
            "RUVIO_DATA_DIR": directory,
        }
        args = [str(binary)]
    else:
        env = os.environ.copy()
        args = [str(binary), "--bind", "127.0.0.1", "--port", str(port),
                "--save", "", "--appendonly", "no", "--daemonize", "no", "--dir", directory]
    server = subprocess.Popen(args, cwd=directory, env=env, stdout=log, stderr=subprocess.STDOUT)
    for _ in range(100):
        if server.poll() is not None:
            raise RuntimeError(f"{name} exited early (see {log.name})")
        try:
            if command(port, "PING").strip() == "PONG":
                break
        except subprocess.CalledProcessError:
            pass
        time.sleep(0.1)
    else:
        raise TimeoutError(f"{name} did not start (see {log.name})")
    time.sleep(0.4)
    return server


def stop_server(server):
    if server.poll() is None:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()


def measure(name, binary, seconds, passes, pause_ms, mode="full", idle_seconds=0, vmmap=False):
    port = available_port()
    with tempfile.TemporaryDirectory(prefix=f"ruvio-memory-{name}-") as directory:
        with open(Path(directory) / "server.log", "w") as log:
            if name == "ruvio":
                env = {
                    **{key: value for key, value in os.environ.items() if not key.startswith("RUVIO_")},
                    "RUVIO_ADDR": f"127.0.0.1:{port}",
                    "RUVIO_SHARDS": "1",
                    "RUVIO_DURABILITY": "memory",
                    "RUVIO_DATA_DIR": directory,
                }
                args = [str(binary)]
            else:
                env = os.environ.copy()
                args = [str(binary), "--bind", "127.0.0.1", "--port", str(port),
                        "--save", "", "--appendonly", "no", "--daemonize", "no", "--dir", directory]
            server = subprocess.Popen(args, cwd=directory, env=env, stdout=log, stderr=subprocess.STDOUT)
            try:
                for _ in range(100):
                    if server.poll() is not None:
                        raise RuntimeError(f"{name} exited early (see {log.name})")
                    try:
                        if command(port, "PING").strip() == "PONG":
                            break
                    except subprocess.CalledProcessError:
                        pass
                    time.sleep(0.1)
                else:
                    raise TimeoutError(f"{name} did not start (see {log.name})")

                time.sleep(0.4)
                baseline = {"rss_bytes": rss_bytes(server.pid), "info": memory_info(port)}
                if vmmap:
                    baseline["vmmap"] = vmmap_summary(server.pid)
                workload_args = ["python3", str(ROOT / "scripts/soak.py"), "--port", str(port),
                                 "--pause-ms", str(pause_ms), "--mode", mode]
                workload_args.extend(["--passes", str(passes)] if passes is not None
                                     else ["--seconds", str(seconds)])
                started = time.monotonic()
                with subprocess.Popen(
                    workload_args,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                ) as workload:
                    peak = baseline["rss_bytes"]
                    while workload.poll() is None:
                        peak = max(peak, rss_bytes(server.pid))
                        time.sleep(0.05)
                    stdout, stderr = workload.communicate()
                    if workload.returncode != 0:
                        raise RuntimeError(f"{name} workload failed: {stderr}")
                elapsed = time.monotonic() - started
                time.sleep(0.5)
                after = {"rss_bytes": rss_bytes(server.pid), "info": memory_info(port)}
                if vmmap:
                    after["vmmap"] = vmmap_summary(server.pid)
                command(port, "FLUSHALL")
                time.sleep(1)
                cleared = {"rss_bytes": rss_bytes(server.pid), "info": memory_info(port)}
                if vmmap:
                    cleared["vmmap"] = vmmap_summary(server.pid)
                result = {
                    "engine": name,
                    "mode": mode,
                    "passes": int(stdout.split("passes=")[1].splitlines()[0]),
                    "elapsed_seconds": round(elapsed, 3),
                    "baseline": baseline,
                    "sampled_peak_rss_bytes": peak,
                    "after": after,
                    "after_flush": cleared,
                }
                if idle_seconds:
                    time.sleep(idle_seconds)
                    result["after_idle"] = {"rss_bytes": rss_bytes(server.pid), "info": memory_info(port)}
                    if vmmap:
                        result["after_idle"]["vmmap"] = vmmap_summary(server.pid)
                return result
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
    parser.add_argument("--redis", type=Path, default=Path(shutil.which("redis-server") or "redis-server"))
    duration = parser.add_mutually_exclusive_group()
    duration.add_argument("--seconds", type=float)
    duration.add_argument("--passes", type=int, help="run the same number of passes per engine")
    parser.add_argument("--pause-ms", type=float, default=10)
    parser.add_argument("--mode", choices=("full", "data", "connections", "slow"), default="full")
    parser.add_argument("--engine", choices=("both", "ruvio", "redis"), default="both")
    parser.add_argument("--idle-seconds", type=float, default=0)
    parser.add_argument("--vmmap", action="store_true", help="capture macOS region resident bytes")
    parser.add_argument("--delete-half", type=int, metavar="KEYS",
                        help="load KEYS strings, delete a random half, report the RSS drop")
    parser.add_argument("--value-size", type=int, default=32)
    parser.add_argument("--settle-seconds", type=float, default=30)
    args = parser.parse_args()
    if args.delete_half:
        results = {}
        for engine, binary in (("ruvio", args.ruvio), ("redis", args.redis)):
            if args.engine in ("both", engine):
                results[engine] = measure_delete_half(engine, binary.resolve(), args.delete_half,
                                                      args.value_size, args.settle_seconds, args.vmmap)
        print(json.dumps(results, indent=2))
        return
    if args.pause_ms < 0 or (args.seconds is not None and args.seconds <= 0) or args.idle_seconds < 0:
        parser.error("--pause-ms and --idle-seconds must be nonnegative; --seconds must be positive")
    if args.vmmap and not shutil.which("vmmap"):
        parser.error("--vmmap requires vmmap")
    if args.passes is not None and args.passes <= 0:
        parser.error("--passes must be positive")
    seconds = 12 if args.seconds is None else args.seconds
    results = {"seconds_per_engine": seconds if args.passes is None else None,
               "passes_per_engine": args.passes, "pause_ms": args.pause_ms,
               "idle_seconds": args.idle_seconds, "mode": args.mode}
    for engine, binary in (("ruvio", args.ruvio), ("redis", args.redis)):
        if args.engine in ("both", engine):
            results[engine] = measure(engine, binary.resolve(), seconds, args.passes, args.pause_ms,
                                      args.mode, args.idle_seconds, args.vmmap)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
