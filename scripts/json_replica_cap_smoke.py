#!/usr/bin/env python3
"""A connected replica that stops reading must not pin the primary's WAL without limit.

Starts a primary (replica_retain_max_bytes = 1 MiB) and a replica, freezes the replica with
SIGSTOP, writes ~25 MiB of JSON path writes, checkpoints, and checks that the primary dropped the
hold (INFO replica_retain_dropped) and kept its WAL small. Then it resumes the replica and checks
that it converges to the primary's document through a fresh snapshot, with no catch-up marker left.

Run after `cargo build --profile release-fast` (or release). Python 3, `ps` (macOS/Linux).
"""

import importlib.util
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("smoke", ROOT / "scripts/json_replica_backlog_smoke.py")
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)

BINARY = Path(os.environ.get("RUVIO_BIN", ROOT / "target/release-fast/ruvio"))
CAP = int(os.environ.get("CAP", 1 << 20))


def wal_bytes(directory):
    return sum(p.stat().st_size for p in (directory / "wal").rglob("*.wal"))


def main():
    assert BINARY.is_file(), BINARY
    with tempfile.TemporaryDirectory(prefix="ruvio-cap-") as temporary:
        work = Path(temporary)
        primary_port, replica_port, replication_port = (smoke.free_port() for _ in range(3))
        processes, logs = {}, []

        def start(name, port, role):
            config = work / f"{name}.toml"
            field = "listen" if role == "primary" else "primary"
            config.write_text(
                f'[replication]\nrole = "{role}"\n{field} = "127.0.0.1:{replication_port}"\n'
            )
            log = (work / f"{name}.log").open("wb")
            logs.append(log)
            processes[name] = subprocess.Popen(
                [str(BINARY), "--config", str(config), "--address", f"127.0.0.1:{port}",
                 "--data-dir", str(work / f"{name}-data"), "--durability", "everysec",
                 "--snapshot-interval-ms", "3600000", "--wal-segment-bytes", "262144",
                 "--protected-mode", "false"],
                cwd=work, stdout=log, stderr=log,
                env={**os.environ, "RUVIO_REPLICA_RETAIN_MAX_BYTES": str(CAP)},
            )
            smoke.wait_for(port, lambda _: True)

        try:
            start("primary", primary_port, "primary")
            start("replica", replica_port, "replica")
            doc = json.dumps({"padding": "p" * 65536, "field": "start"}).encode()
            assert smoke.query(primary_port, b"JSON.SET", b"{doc}", b"$", doc) == b"OK"
            smoke.wait_for(replica_port, lambda r: r.get("wal_last_sequence")
                           == smoke.info(primary_port).get("wal_last_sequence"))
            processes["replica"].send_signal(signal.SIGSTOP)
            smoke.writes(primary_port, 0, 25000)
            for _ in range(3):
                assert smoke.query(primary_port, b"SAVE") == b"OK"
            primary_info = smoke.info(primary_port)
            dropped = int(primary_info["replica_retain_dropped"])
            wal = wal_bytes(work / "primary-data")
            assert dropped > 0 or CAP == 0, primary_info
            assert CAP == 0 or wal < 8 * CAP, f"primary WAL {wal} bytes with a {CAP} byte cap"
            processes["replica"].send_signal(signal.SIGCONT)
            smoke.writes(primary_port, 25000, 200)
            smoke.wait_for(
                replica_port,
                lambda r: r.get("wal_last_sequence") == smoke.info(primary_port).get("wal_last_sequence"),
                timeout=60,
            )
            value = smoke.query(replica_port, b"JSON.GET", b"{doc}", b"$.field")
            assert json.loads(value) == "00025199" + "x" * 1024, value[:60]
            assert not list((work / "replica-data").glob("REPLICA_CATCHUP_*"))
            print(json.dumps({"replica_retain_dropped": dropped, "primary_wal_bytes": wal,
                              "cap_bytes": CAP, "converged": True}, indent=2))
        finally:
            for p in processes.values():
                if p.poll() is None:
                    p.send_signal(signal.SIGCONT)
                    p.terminate()
            for p in processes.values():
                try:
                    p.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    p.kill()
                    p.wait()
            for log in logs:
                log.close()


if __name__ == "__main__":
    sys.exit(main())
