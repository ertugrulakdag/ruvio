#!/usr/bin/env python3
"""Exercise a large JSON path-WAL backlog across two live Ruvio processes.

Run after `cargo build --profile release-fast` (or release). Requires Python 3 and `ps` (macOS/Linux).
All data and configuration live in a temporary directory.
"""

import json
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path

BINARY = Path(__file__).resolve().parents[1] / "target/release-fast/ruvio"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def frame(*args):
    return b"*%d\r\n" % len(args) + b"".join(
        b"$%d\r\n%s\r\n" % (len(arg), arg) for arg in args
    )


def query(port, *args):
    with socket.create_connection(("127.0.0.1", port), timeout=25) as sock:
        sock.settimeout(25)
        sock.sendall(frame(*args))
        stream = sock.makefile("rb")
        kind, line = stream.read(1), stream.readline().strip()
        if kind == b"-":
            raise RuntimeError(line)
        if kind == b"$":
            return None if line == b"-1" else stream.read(int(line) + 2)[:-2]
        if kind in (b"+", b":"):
            return line
        raise RuntimeError((kind, line))


def info(port):
    return dict(
        line.split(":", 1)
        for line in query(port, b"INFO").decode().splitlines()
        if ":" in line and not line.startswith("#")
    )


def writes(port, start, count):
    with socket.create_connection(("127.0.0.1", port), timeout=25) as sock:
        sock.settimeout(25)
        stream = sock.makefile("rb")
        for base in range(start, start + count, 128):
            batch = min(128, start + count - base)
            sock.sendall(
                b"".join(
                    frame(
                        b"JSON.SET",
                        b"{doc}",
                        b"$.field",
                        json.dumps(f"{index:08d}" + "x" * 1024).encode(),
                    )
                    for index in range(base, base + batch)
                )
            )
            for _ in range(batch):
                reply = stream.readline()
                if reply != b"+OK\r\n":
                    raise RuntimeError(f"write {base}: {reply!r}")


def wait_for(port, predicate, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate(info(port)):
                return
        except (OSError, RuntimeError):
            pass
        time.sleep(0.1)
    raise TimeoutError((port, info(port)))


def process_rss_kib(pid):
    return int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True).strip())


def main():
    assert BINARY.is_file(), f"build the release binary first: {BINARY}"
    with tempfile.TemporaryDirectory(prefix="ruvio-json-backlog-") as temporary:
        work = Path(temporary)
        primary_port, replica_port, replication_port = (free_port() for _ in range(3))
        assert len({primary_port, replica_port, replication_port}) == 3
        processes = {}
        logs = {}

        def start(name, port, role):
            config = work / f"{name}.toml"
            address_field = "listen" if role == "primary" else "primary"
            config.write_text(
                f'[replication]\nrole = "{role}"\n'
                f'{address_field} = "127.0.0.1:{replication_port}"\n'
            )
            logs[name] = (work / f"{name}.log").open("wb")
            processes[name] = subprocess.Popen(
                [
                    str(BINARY), "--config", str(config),
                    "--address", f"127.0.0.1:{port}",
                    "--data-dir", str(work / f"{name}-data"),
                    "--durability", "everysec",
                    "--snapshot-interval-ms", "3600000",
                    "--wal-segment-bytes", "2097152",
                    "--protected-mode", "false",
                ],
                cwd=work,
                stdout=logs[name],
                stderr=logs[name],
            )
            wait_for(port, lambda _: True)

        try:
            start("primary", primary_port, "primary")
            start("replica", replica_port, "replica")
            document = json.dumps({"padding": "p" * 524288, "field": "start"}).encode()
            assert query(primary_port, b"JSON.SET", b"{doc}", b"$", document) == b"OK"
            wait_for(
                replica_port,
                lambda replica: replica.get("wal_last_sequence")
                == info(primary_port).get("wal_last_sequence"),
            )
            old_cursor = int(info(replica_port)["wal_last_sequence"])
            processes["replica"].terminate()
            processes["replica"].wait(timeout=15)

            writes(primary_port, 0, 2000)
            assert query(primary_port, b"SAVE") == b"OK"
            snapshot_sequence = int(info(primary_port)["wal_last_sequence"])
            writes(primary_port, 2000, 35000)
            segments = sorted((work / "primary-data" / "wal").rglob("*.wal"))
            assert segments and min(int(segment.stem) for segment in segments) > old_cursor, (
                "the old replica cursor must be compacted out"
            )
            backlog_bytes = sum(segment.stat().st_size for segment in segments)
            assert backlog_bytes > 16 * 1024 * 1024, backlog_bytes
            assert snapshot_sequence < int(info(primary_port)["wal_last_sequence"])

            primary_rss = process_rss_kib(processes["primary"].pid)
            started = time.monotonic()
            writer_error = []

            def keep_writing():
                try:
                    writes(primary_port, 37000, 10000)
                except Exception as error:
                    writer_error.append(error)

            writer = threading.Thread(target=keep_writing)
            writer.start()
            start("replica", replica_port, "replica")
            writer.join(timeout=45)
            assert not writer.is_alive() and not writer_error, writer_error
            assert query(primary_port, b"WAIT", b"1", b"25000") == b"1", (
                info(primary_port), info(replica_port)
            )
            wait_for(
                replica_port,
                lambda replica: replica.get("wal_last_sequence")
                == info(primary_port).get("wal_last_sequence"),
            )
            value = query(replica_port, b"JSON.GET", b"{doc}", b"$.field")
            assert json.loads(value) == "00046999" + "x" * 1024, value[:80]
            assert not list((work / "replica-data").glob("REPLICA_CATCHUP_*"))
            print(json.dumps({
                "old_cursor": old_cursor,
                "snapshot_sequence": snapshot_sequence,
                "backlog_bytes": backlog_bytes,
                "primary_rss_kib": primary_rss,
                "replica_rss_kib": process_rss_kib(processes["replica"].pid),
                "recovery_seconds": round(time.monotonic() - started, 2),
                "final_sequence": info(replica_port)["wal_last_sequence"],
            }, indent=2))
        finally:
            for process in processes.values():
                if process.poll() is None:
                    process.terminate()
            for process in processes.values():
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            for log in logs.values():
                log.close()


if __name__ == "__main__":
    main()
