#!/usr/bin/env python3
"""Compare two Ruvio binaries with random-key LRU reads and optional TTL churn.

Example: python3 scripts/compare_lru_reads.py /path/to/before target/release/ruvio --mixed
Requires redis-benchmark; emits one JSON line per variant and paired round.
"""

import argparse
import json
import re
import socket
import subprocess
import tempfile
from pathlib import Path

from redis_compare import Server


def load_strings(port):
    with socket.create_connection(("127.0.0.1", port), timeout=10) as sock:
        for first in range(0, 100_000, 200):
            batch = b"".join(
                b"*3\r\n$3\r\nSET\r\n$13\r\nk%012d\r\n$64\r\n%s\r\n"
                % (index, b"x" * 64)
                for index in range(first, first + 200)
            )
            sock.sendall(batch)
            remaining = 200 * len(b"+OK\r\n")
            while remaining > 0:
                chunk = sock.recv(65536)
                if not chunk:
                    raise ConnectionError("server disconnected during preload")
                remaining -= len(chunk)
            if remaining != 0:
                raise RuntimeError("unexpected preload response")


def benchmark(argv):
    output = subprocess.run(argv, capture_output=True, text=True, check=True).stdout
    rate = re.search(r"throughput summary: ([\d.]+) requests per second", output)
    latency = re.search(
        r"avg\s+min\s+p50\s+p95\s+p99\s+max\s+"
        r"([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)",
        output,
    )
    if not rate or not latency:
        raise RuntimeError(output[-2000:])
    return float(rate[1]), float(latency[5])


def run(binary, iteration, mixed):
    with tempfile.TemporaryDirectory() as temp:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        server = Server(
            "ruvio",
            [str(binary), "--address", f"127.0.0.1:{port}", "--durability", "memory",
             "--shards", "1", "--data-dir", str(Path(temp) / "data"),
             "--maxmemory-policy", "allkeys-lru"],
            port, Path(temp),
        )
        try:
            load_strings(port)
            with socket.create_connection(("127.0.0.1", port)) as check:
                check.sendall(b"*2\r\n$3\r\nGET\r\n$13\r\nk000000000042\r\n")
                if not check.recv(128).startswith(b"$64\r\n"):
                    raise RuntimeError("preloaded GET is not a cache hit")
            common = ["redis-benchmark", "-h", "127.0.0.1", "-p", str(port),
                      "-c", "50", "-P", "16", "-r", "100000"]
            get = ["get", "k__rand_int__"]
            result = {"binary": str(binary), "round": iteration, "rss_kib": server.rss_kib()}
            result["get_rps"], result["get_p99_ms"] = benchmark(
                common + ["-n", "1000000"] + get
            )
            if mixed:
                writer = subprocess.Popen(
                    ["redis-benchmark", "-h", "127.0.0.1", "-p", str(port),
                     "-c", "25", "-P", "8", "-r", "100000", "-n", "2000000",
                     "set", "k__rand_int__", "v", "PX", "1000"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                )
                try:
                    read = ["redis-benchmark", "-h", "127.0.0.1", "-p", str(port),
                            "-c", "25", "-P", "8", "-r", "100000", "-n", "2000000"] + get
                    result["mixed_get_rps"], result["mixed_get_p99_ms"] = benchmark(read)
                    written, errors = writer.communicate(timeout=30)
                    if writer.returncode:
                        raise RuntimeError(errors)
                    match = re.search(r"throughput summary: ([\d.]+) requests per second", written)
                    if not match:
                        raise RuntimeError(written[-2000:])
                    result["mixed_set_rps"] = float(match[1])
                    result["mixed_rss_kib"] = server.rss_kib()
                finally:
                    if writer.poll() is None:
                        writer.terminate()
                        writer.wait(timeout=3)
            return result
        finally:
            server.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("old", type=Path)
    parser.add_argument("new", type=Path)
    parser.add_argument("--rounds", type=int, default=6)
    parser.add_argument("--mixed", action="store_true")
    args = parser.parse_args()
    for round_number in range(1, args.rounds + 1):
        order = (args.old, args.new) if round_number % 2 else (args.new, args.old)
        for binary in order:
            print(json.dumps(run(binary.resolve(), round_number, args.mixed)), flush=True)
