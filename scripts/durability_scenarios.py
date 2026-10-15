#!/usr/bin/env python3
"""Crash and restart checks for Ruvio durability profiles.

Each scenario starts a private data directory, kills the process with SIGKILL,
starts it again, and checks the key. These are pass/fail checks, not benchmarks.
"""

from __future__ import annotations

import argparse
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from resp_conformance import encode, read_response


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def free_cluster_port() -> int:
    # Two data ports plus the cluster bus at base+10000 must stay within 65535.
    for port in range(26000, 45000):
        probes = []
        try:
            for offset in (0, 1):
                probe = socket.socket()
                probe.bind(("127.0.0.1", port + offset))
                probes.append(probe)
        except OSError:
            for probe in probes:
                probe.close()
            continue
        for probe in probes:
            probe.close()
        return port
    raise RuntimeError("no free two-shard port")


def command(port: int, *arguments: bytes):
    with socket.create_connection(("127.0.0.1", port), timeout=2) as client:
        client.sendall(encode(*arguments))
        return read_response(client.makefile("rb"))


def wait_ready(port: int) -> None:
    for _ in range(100):
        try:
            if command(port, b"PING") == (b"+", b"PONG"):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"ruvio on {port} did not become ready")


class Server:
    def __init__(self, binary: str, port: int, profile: str, data_dir: Path, extra: list[str], env: dict[str, str]):
        self.binary = binary
        self.port = port
        self.profile = profile
        self.data_dir = data_dir
        self.extra = extra
        self.env = env
        self.process: subprocess.Popen | None = None
        self.log_path = data_dir / "server.log"

    def start(self) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        log = self.log_path.open("ab")
        command_line = [
            self.binary,
            "--address",
            f"127.0.0.1:{self.port}",
            "--durability",
            self.profile,
            "--data-dir",
            str(self.data_dir),
            "--protected-mode",
            "false",
            *self.extra,
        ]
        self.process = subprocess.Popen(
            command_line,
            stdout=log,
            stderr=log,
            env=self.env,
            start_new_session=True,
        )
        try:
            wait_ready(self.port)
        except Exception:
            self.kill()
            raise

    def kill(self) -> None:
        process = self.process
        if process is None or process.poll() is not None:
            return
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)
        self.process = None


def expect(port: int, arguments: tuple[bytes, ...], expected) -> None:
    actual = command(port, *arguments)
    if actual != expected:
        raise AssertionError(f"{arguments!r}: got {actual!r}, expected {expected!r}")


def scenario_memory(server: Server) -> None:
    server.start()
    expect(server.port, (b"SET", b"kept", b"1"), (b"+", b"OK"))
    server.kill()
    server.start()
    expect(server.port, (b"GET", b"kept"), (b"$", None))


def scenario_snapshot_without_save(server: Server) -> None:
    server.start()
    expect(server.port, (b"SET", b"kept", b"1"), (b"+", b"OK"))
    server.kill()
    server.start()
    expect(server.port, (b"GET", b"kept"), (b"$", None))


def scenario_snapshot_save(server: Server) -> None:
    server.start()
    expect(server.port, (b"SET", b"kept", b"saved"), (b"+", b"OK"))
    expect(server.port, (b"SAVE",), (b"+", b"OK"))
    server.kill()
    server.start()
    expect(server.port, (b"GET", b"kept"), (b"$", b"saved"))


def scenario_always(server: Server) -> None:
    server.start()
    expect(server.port, (b"SET", b"kept", b"durable"), (b"+", b"OK"))
    server.kill()
    server.start()
    expect(server.port, (b"GET", b"kept"), (b"$", b"durable"))


def scenario_everysec(server: Server) -> None:
    server.start()
    expect(server.port, (b"SET", b"kept", b"synced"), (b"+", b"OK"))
    time.sleep(1.5)
    server.kill()
    server.start()
    expect(server.port, (b"GET", b"kept"), (b"$", b"synced"))


def scenario_px_expires_across_restart(server: Server) -> None:
    server.start()
    expect(server.port, (b"SET", b"kept", b"temp", b"PX", b"400"), (b"+", b"OK"))
    expect(server.port, (b"SAVE",), (b"+", b"OK"))
    time.sleep(0.7)
    server.kill()
    server.start()
    expect(server.port, (b"GET", b"kept"), (b"$", None))


def scenario_hot_keys_save(server: Server) -> None:
    server.start()
    other = server.port + 1
    wait_ready(other)
    expect(server.port, (b"SET", b"lock:shared", b"held"), (b"+", b"OK"))
    expect(other, (b"GET", b"lock:shared"), (b"$", b"held"))
    expect(server.port, (b"SAVE",), (b"+", b"OK"))
    expect(other, (b"SAVE",), (b"+", b"OK"))
    server.kill()
    server.start()
    wait_ready(other)
    expect(server.port, (b"GET", b"lock:shared"), (b"$", b"held"))
    expect(other, (b"GET", b"lock:shared"), (b"$", b"held"))


SCENARIOS = {
    "memory": ("memory", [], {}, scenario_memory),
    "snapshot-without-save": ("snapshot", [], {}, scenario_snapshot_without_save),
    "snapshot-save": ("snapshot", [], {}, scenario_snapshot_save),
    "always": ("always", [], {}, scenario_always),
    "everysec": ("everysec", [], {}, scenario_everysec),
    "px-expires-across-restart": ("snapshot", [], {}, scenario_px_expires_across_restart),
    "hot-keys-save": (
        "snapshot",
        ["--shards", "2", "--advertised-host", "127.0.0.1"],
        {"RUVIO_HOT_KEYS": "lock:*"},
        scenario_hot_keys_save,
    ),
}


def run_one(binary: str, name: str) -> None:
    profile, extra, env_extra, function = SCENARIOS[name]
    port = free_cluster_port() if "--shards" in extra else free_port()
    with tempfile.TemporaryDirectory(prefix=f"ruvio-{name}-") as directory:
        env = os.environ.copy()
        env.update(env_extra)
        env["RUST_LOG"] = "error"
        server = Server(binary, port, profile, Path(directory), extra, env)
        try:
            function(server)
        except Exception as error:
            tail = ""
            if server.log_path.exists():
                tail = server.log_path.read_bytes()[-2000:].decode("utf-8", "replace")
            raise RuntimeError(f"{error}\n{tail}") from error
        finally:
            server.kill()
    print(f"ok {name}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin", default="target/release-fast/ruvio")
    parser.add_argument("--scenario", choices=[*SCENARIOS, "all"], default="all")
    args = parser.parse_args()
    binary = str(Path(args.bin).resolve())
    if not Path(binary).is_file():
        print(f"ruvio binary not found: {binary}", file=sys.stderr)
        sys.exit(2)
    names = list(SCENARIOS) if args.scenario == "all" else [args.scenario]
    failed = False
    for name in names:
        try:
            run_one(binary, name)
        except Exception as error:  # noqa: BLE001 - report every scenario
            failed = True
            print(f"fail {name}: {error}", file=sys.stderr)
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
