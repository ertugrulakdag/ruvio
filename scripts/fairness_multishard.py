#!/usr/bin/env python3
"""Compare both shard tails while one owner serves large replies and Pub/Sub."""

import argparse
import json
import os
import random
import socket
import subprocess
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from benchmark_shards import tag_for_shard
from fairness_probe import buffer_peaks, measure_phase, request, seed
from memory_churn import ROOT
from soak import command, connect, read_reply


def free_port_pair():
    for _ in range(100):
        with socket.socket() as first, socket.socket() as second, \
                socket.socket() as first_bus, socket.socket() as second_bus:
            port = random.randrange(20000, 45000)
            try:
                first.bind(("127.0.0.1", port))
                second.bind(("127.0.0.1", port + 1))
                first_bus.bind(("127.0.0.1", port + 10000))
                second_bus.bind(("127.0.0.1", port + 10001))
            except OSError:
                continue
            return port
    raise OSError("no consecutive loopback ports available")


def wait_ready(server, ports, log_path):
    for _ in range(100):
        if server.poll() is not None:
            raise RuntimeError(f"server exited: {log_path.read_text()}")
        try:
            if all(request(port, b"PING") == b"+PONG\r\n" for port in ports):
                return
        except (OSError, ConnectionError):
            pass
        time.sleep(.1)
    raise TimeoutError("two-shard server did not start")


def run(binary, seconds):
    base = free_port_pair()
    ports = (base, base + 1)
    keys = []
    for shard in range(2):
        tag = tag_for_shard(shard, 2).encode()
        keys.append({name: b"{" + tag + b"}:" + name.encode()
                     for name in ("small", "list", "large")})
    with tempfile.TemporaryDirectory(prefix="ruvio-fairness-two-") as directory:
        log_path = Path(directory) / "server.log"
        env = {key: value for key, value in os.environ.items() if not key.startswith("RUVIO_")}
        env.update(RUVIO_ADDR=f"127.0.0.1:{base}", RUVIO_SHARDS="2", RUVIO_DURABILITY="memory",
                   RUVIO_DATA_DIR=directory, RUVIO_MAX_PIPELINE_COMMANDS="4",
                   RUVIO_WRITE_TIMEOUT_MS="1500")
        with log_path.open("w") as log:
            server = subprocess.Popen([str(binary)], cwd=directory, env=env,
                                      stdout=log, stderr=subprocess.STDOUT)
            try:
                wait_ready(server, ports, log_path)
                for port, shard_keys in zip(ports, keys):
                    seed(port, *(shard_keys[name] for name in ("small", "list", "large")))

                def measure(compete):
                    with ThreadPoolExecutor(max_workers=2) as pool:
                        futures = [pool.submit(measure_phase, port, server.pid, seconds,
                                               compete and shard == 0,
                                               small_key=keys[shard]["small"],
                                               list_key=keys[shard]["list"],
                                               large_key=keys[shard]["large"])
                                   for shard, port in enumerate(ports)]
                        return [future.result() for future in futures]

                baseline = measure(False)
                baseline_peaks = [buffer_peaks(port) for port in ports]
                with connect("127.0.0.1", ports[0]) as subscriber:
                    subscriber.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
                    subscriber.sendall(command(b"SUBSCRIBE", b"fairness"))
                    if b"fairness" not in read_reply(subscriber):
                        raise AssertionError("Pub/Sub subscription failed")
                    published = []
                    failures = []

                    def publish():
                        try:
                            time.sleep(.1)
                            frame = command(b"PUBLISH", b"fairness", b"p" * (64 * 1024))
                            with connect("127.0.0.1", ports[0]) as publisher:
                                publisher.sendall(frame * 64)
                                for _ in range(64):
                                    reply = read_reply(publisher)
                                    if reply not in (b":0\r\n", b":1\r\n"):
                                        raise AssertionError(f"unexpected publish reply: {reply!r}")
                                    published.append(reply == b":1\r\n")
                        except Exception as error:
                            failures.append(error)

                    worker = threading.Thread(target=publish)
                    worker.start()
                    try:
                        mixed = measure(True)
                    finally:
                        worker.join(timeout=5)
                        if worker.is_alive():
                            raise TimeoutError("publisher did not finish")
                    if failures:
                        raise failures[0]
                    if not any(published):
                        raise AssertionError("no publication reached the subscriber")
                mixed_peaks = [buffer_peaks(port) for port in ports]
                if mixed_peaks[0]["pubsub_queue_peak_bytes"] == 0:
                    raise AssertionError("no real Pub/Sub queue observed")
                for shard in range(2):
                    baseline[shard]["buffer_peaks"] = baseline_peaks[shard]
                    mixed[shard]["buffer_peaks"] = mixed_peaks[shard]
                return {"baseline_by_shard": baseline, "mixed_by_shard": mixed,
                        "published_messages": len(published),
                        "delivered_messages": sum(published)}
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
    parser.add_argument("--seconds", type=float, default=3)
    args = parser.parse_args()
    if args.seconds <= 0:
        parser.error("--seconds must be positive")
    print(json.dumps(run(args.ruvio.resolve(), args.seconds), indent=2))


if __name__ == "__main__":
    main()
