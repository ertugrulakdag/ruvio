#!/usr/bin/env python3
"""Exercise a non-reading TCP client and recovery on an isolated Ruvio server."""

import argparse
import json
import os
import socket
import subprocess
import tempfile
import time
from pathlib import Path

from memory_churn import ROOT, available_port, command as cli_command, memory_info, rss_bytes
from soak import command, connect, read_reply


def request(port, *parts):
    with connect("127.0.0.1", port) as client:
        client.sendall(command(*parts))
        return read_reply(client)


def stats(port):
    reply = cli_command(port, "INFO", "stats")
    return {key: int(value) for line in reply.splitlines() if ":" in line
            for key, value in [line.split(":", 1)] if key == "total_commands_processed"}


def checked_memory(port):
    memory = memory_info(port)
    components = ("key_memory_bytes", "value_memory_bytes", "collection_memory_bytes",
                  "index_memory_bytes", "pooled_value_bytes")
    if memory.get("charged_memory_bytes") != sum(memory[name] for name in components):
        raise AssertionError(f"storage memory breakdown disagrees with charge: {memory}")
    return memory


def measure(binary, wait, cycles):
    port = available_port()
    with tempfile.TemporaryDirectory(prefix="ruvio-backpressure-") as directory:
        log_path = Path(directory) / "server.log"
        env = {key: value for key, value in os.environ.items() if not key.startswith("RUVIO_")}
        env.update(RUVIO_ADDR=f"127.0.0.1:{port}", RUVIO_SHARDS="1",
                   RUVIO_DURABILITY="memory", RUVIO_DATA_DIR=directory,
                   RUVIO_MAXMEMORY=str(4 * 1024 * 1024),
                   RUVIO_MAX_SHARD_BYTES=str(4 * 1024 * 1024),
                   RUVIO_MEMORY_HEADROOM_BYTES="0", RUVIO_MAX_CONNECTIONS="4",
                   RUVIO_MAX_OUTPUT_BUFFER_BYTES=str(1024 * 1024),
                   RUVIO_MAX_PIPELINE_COMMANDS="4", RUVIO_WRITE_TIMEOUT_MS="1500")
        with log_path.open("w") as log:
            server = subprocess.Popen([str(binary)], cwd=directory, env=env,
                                      stdout=log, stderr=subprocess.STDOUT)
            try:
                for _ in range(100):
                    if server.poll() is not None:
                        raise RuntimeError(f"server exited early: {log_path.read_text()}")
                    try:
                        if request(port, b"PING") == b"+PONG\r\n":
                            break
                    except (OSError, ConnectionError):
                        pass
                    time.sleep(0.1)
                else:
                    raise TimeoutError("server did not start")

                value = b"x" * (128 * 1024)
                if request(port, b"SET", b"pressure", value) != b"+OK\r\n":
                    raise AssertionError("could not seed pressure key")
                baseline = {"rss_bytes": rss_bytes(server.pid), "memory": checked_memory(port)}
                holders = [connect("127.0.0.1", port) for _ in range(4)]
                try:
                    time.sleep(0.1)
                    denied = request(port, b"PING")
                    if not denied.startswith(b"-ERR max number of clients reached"):
                        raise AssertionError(f"connection cap not enforced: {denied!r}")
                finally:
                    for holder in holders:
                        holder.close()
                time.sleep(0.1)
                if request(port, b"PING") != b"+PONG\r\n":
                    raise AssertionError("connection cap did not recover")

                slow = connect("127.0.0.1", port)
                slow.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
                slow.sendall(command(b"GET", b"pressure") * 256)
                samples = []
                for _ in range(3):
                    time.sleep(wait / 3)
                    if request(port, b"PING") != b"+PONG\r\n":
                        raise AssertionError("slow reader blocked peer PING")
                    samples.append({"rss_bytes": rss_bytes(server.pid),
                                    "memory": checked_memory(port), "stats": stats(port)})
                slow.close()
                time.sleep(1.7)
                after_close = {"rss_bytes": rss_bytes(server.pid), "memory": checked_memory(port)}
                if request(port, b"GET", b"pressure") != b"$131072\r\n" + value + b"\r\n":
                    raise AssertionError("key unavailable after slow reader")
                large = b"z" * (3 * 1024 * 1024)
                if request(port, b"SET", b"large", large) != b"+OK\r\n":
                    raise AssertionError("first large write was rejected")
                rejected = request(port, b"SET", b"recovered", large)
                if not rejected.startswith(b"-OOM "):
                    raise AssertionError(f"process/shard cap did not reject write: {rejected!r}")
                if request(port, b"GET", b"recovered") != b"$-1\r\n":
                    raise AssertionError("rejected write became visible")
                if request(port, b"DEL", b"large") != b":1\r\n":
                    raise AssertionError("failed to remove large key")
                if request(port, b"SET", b"recovered", large) != b"+OK\r\n":
                    raise AssertionError("large write did not recover after deletion")
                if request(port, b"DEL", b"pressure", b"recovered") != b":2\r\n":
                    raise AssertionError("failed to remove pressure keys")
                cycle_rss = []
                for _ in range(cycles):
                    if request(port, b"SET", b"cycled", large) != b"+OK\r\n":
                        raise AssertionError("cyclic write failed after recovery")
                    if request(port, b"DEL", b"cycled") != b":1\r\n":
                        raise AssertionError("cyclic delete failed")
                    cycle_rss.append(rss_bytes(server.pid))
                cli_command(port, "FLUSHALL")
                time.sleep(1)
                after_flush = {"rss_bytes": rss_bytes(server.pid), "memory": checked_memory(port)}
                time.sleep(2)
                after_idle = {"rss_bytes": rss_bytes(server.pid), "memory": checked_memory(port)}
                if after_flush["memory"].get("charged_memory_bytes") != 0:
                    raise AssertionError("charged memory remained after FLUSHALL")
                logs = log_path.read_text()
                if "write timed out" not in logs:
                    raise AssertionError("slow reader did not produce a write timeout")
                if "output buffer limit exceeded" in logs:
                    raise AssertionError("output cap fired before socket backpressure")
                return {"baseline": baseline, "samples": samples, "after_close": after_close,
                        "cycle_rss_bytes": cycle_rss, "after_flush": after_flush,
                        "after_idle": after_idle, "connection_limit_rejected": True,
                        "connection_limit_recovered": True, "write_timed_out": True,
                        "output_limit_fired": False}
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
    parser.add_argument("--ruvio", type=Path, default=ROOT / "target/release-fast/ruvio")
    parser.add_argument("--hold-seconds", type=float, default=3)
    parser.add_argument("--cycles", type=int, default=12)
    args = parser.parse_args()
    if args.hold_seconds < 2 or args.cycles < 1:
        parser.error("--hold-seconds must be at least 2 and --cycles positive")
    print(json.dumps(measure(args.ruvio.resolve(), args.hold_seconds, args.cycles), indent=2))


if __name__ == "__main__":
    main()
