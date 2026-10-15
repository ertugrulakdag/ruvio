#!/usr/bin/env python3
"""Disk fault scenarios for the WAL (Linux, root): write errors and a slow disk.

Builds ext4 on a loop file behind device-mapper (`dm-flakey`, `dm-delay`), runs Ruvio with
`--durability always` on it (satisfied inline or with the sync worker), injects the fault while a
client writes, then checks that every acknowledged key survives a restart on a healthy device and the
keyspace is a prefix.

    sudo RUVIO_BIN=target/release-fast/ruvio python3 scripts/disk_faults.py [error|stall|all] [--worker]

Not a power-loss simulation: acknowledgements are never sent after a failed sync, and the device does
not lose data it already acknowledged."""
import os, random, shutil, socket, subprocess, sys, threading, time
from pathlib import Path

BIN = os.environ.get("RUVIO_BIN", "target/release-fast/ruvio")
PORT = int(os.environ.get("RUVIO_FAULT_PORT", "6591"))
CPU = os.environ.get("RUVIO_FAULT_CPU", "1")
IMG = Path(os.environ.get("RUVIO_FAULT_IMG", "/var/tmp/ruvio-faults.img"))
MNT = Path("/mnt/ruvio-faults")
DM = "ruvio-fault"
WORKER = "--worker" in sys.argv


def sh(*args, check=True, **kw):
    return subprocess.run(args, check=check, capture_output=True, text=True, **kw)


class Device:
    def __init__(self):
        IMG.parent.mkdir(parents=True, exist_ok=True)
        with IMG.open("wb") as f:
            f.truncate(1 << 30)
        self.loop = sh("losetup", "-f", "--show", str(IMG)).stdout.strip()
        self.sectors = int(sh("blockdev", "--getsz", self.loop).stdout)
        sh("dmsetup", "create", DM, "--table", self.table("ok"))
        sh("mkfs.ext4", "-q", "-F", f"/dev/mapper/{DM}")
        MNT.mkdir(parents=True, exist_ok=True)
        self.mount()

    def table(self, kind):
        n, dev = self.sectors, self.loop
        if kind == "ok":
            return f"0 {n} linear {dev} 0"
        if kind == "error":
            return f"0 {n} flakey {dev} 0 0 1 1 error_writes"
        if kind == "stall":
            return f"0 {n} delay {dev} 0 0 {dev} 0 500"
        raise ValueError(kind)

    def mount(self):
        sh("mount", f"/dev/mapper/{DM}", str(MNT))

    def umount(self):
        sh("umount", "-l", str(MNT), check=False)

    def switch(self, kind):
        sh("dmsetup", "suspend", "--noflush", DM, check=False)
        sh("dmsetup", "reload", DM, "--table", self.table(kind))
        sh("dmsetup", "resume", DM)

    def recover(self):
        """Back to a healthy device and a clean filesystem (a write error aborts the ext4 journal)."""
        self.umount()
        self.switch("ok")
        sh("e2fsck", "-fy", f"/dev/mapper/{DM}", check=False)
        self.mount()

    def close(self):
        self.umount()
        sh("dmsetup", "remove", "--force", DM, check=False)
        sh("losetup", "-d", self.loop, check=False)
        IMG.unlink(missing_ok=True)


def start_server(data: Path):
    env = dict(os.environ)
    env.update({"RUVIO_CORE": CPU, "RUVIO_SHARDS": "1", "RUVIO_CPUSET": CPU, "RUVIO_ADDR": f"127.0.0.1:{PORT}",
                "RUVIO_ADVERTISE_HOST": "127.0.0.1", "RUVIO_DURABILITY": "always", "RUVIO_DATA_DIR": str(data)})
    env["RUVIO_SYNC_WORKER"] = "on" if WORKER else "off"
    log = (data.parent / "server.log").open("a")
    return subprocess.Popen(["taskset", "-c", CPU, BIN], env=env, cwd=data.parent, stdout=log, stderr=subprocess.STDOUT)


def ping(timeout=20):
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection(("127.0.0.1", PORT), timeout=0.3) as s:
                s.sendall(b"*1\r\n$4\r\nPING\r\n")
                if s.recv(16).startswith(b"+PONG"):
                    return True
        except OSError:
            time.sleep(0.05)
    return False


def frame(*parts):
    out = f"*{len(parts)}\r\n".encode()
    for p in parts:
        b = p.encode()
        out += f"${len(b)}\r\n".encode() + b + b"\r\n"
    return out


class Writer(threading.Thread):
    """Sends SET k:N one by one (pipeline 8) and counts +OK replies."""

    def __init__(self, seconds):
        super().__init__()
        self.sent = self.acked = 0
        self.errors = []
        self.deadline = time.time() + seconds
        self.stop = False

    def run(self):
        try:
            s = socket.create_connection(("127.0.0.1", PORT), timeout=3)
            s.settimeout(3)
            buf = b""
            while time.time() < self.deadline and not self.stop:
                batch = b""
                n = 8
                for _ in range(n):
                    self.sent += 1
                    batch += frame("SET", f"k:{self.sent}", "1")
                s.sendall(batch)
                got = 0
                while got < n:
                    chunk = s.recv(65536)
                    if not chunk:
                        return
                    buf += chunk
                    while b"\r\n" in buf and got < n:
                        line, buf = buf.split(b"\r\n", 1)
                        got += 1
                        if line == b"+OK":
                            self.acked += 1
                        else:
                            self.errors.append(line[:60].decode(errors="replace"))
                            self.stop = True
                            return
        except OSError as e:
            self.errors.append(f"{type(e).__name__}: {e}")


def present_map(limit):
    present = [False] * (limit + 1)
    with socket.create_connection(("127.0.0.1", PORT), timeout=10) as s:
        s.settimeout(10)
        for start in range(1, limit + 1, 200):
            ids = range(start, min(limit, start + 199) + 1)
            s.sendall(b"".join(frame("EXISTS", f"k:{i}") for i in ids))
            buf, got = b"", 0
            while got < len(ids):
                buf += s.recv(65536)
                while b"\r\n" in buf and got < len(ids):
                    line, buf = buf.split(b"\r\n", 1)
                    present[ids[got]] = line == b":1"
                    got += 1
    return present


def verify(sent, acked, label):
    present = present_map(sent)
    first_missing = next((i for i in range(1, len(present)) if not present[i]), None)
    if first_missing is not None:
        later = next((i for i in range(first_missing + 1, len(present)) if present[i]), None)
        if later is not None:
            raise SystemExit(f"FAIL {label}: hole at {first_missing} while {later} exists")
        if first_missing <= acked:
            raise SystemExit(f"FAIL {label}: acknowledged key {first_missing} missing (acked {acked})")
    print(f"ok {label}: sent={sent} acked={acked} recovered={sum(present[1:])}", flush=True)


def get_latency():
    start = time.perf_counter()
    try:
        with socket.create_connection(("127.0.0.1", PORT), timeout=5) as s:
            s.settimeout(5)
            s.sendall(frame("GET", "unrelated"))
            s.recv(64)
        return time.perf_counter() - start
    except OSError:
        return float("inf")


def scenario(device, kind, trial):
    root = MNT / f"{kind}-{trial}"
    shutil.rmtree(root, ignore_errors=True)
    data = root / "data"
    data.mkdir(parents=True)
    proc = start_server(data)
    assert ping(), f"{kind} {trial}: server did not start"
    writer = Writer(4 if kind == "error" else 6)
    writer.start()
    time.sleep(random.uniform(0.3, 0.8))
    acked_before = writer.acked
    device.switch(kind)
    if kind == "stall":
        latencies = [get_latency() for _ in range(6)]
        print(f"   stall: GET on another connection during a 500 ms write delay: max {max(latencies) * 1000:.0f} ms, "
              f"median {sorted(latencies)[len(latencies) // 2] * 1000:.0f} ms", flush=True)
        device.switch("ok")
    writer.join(timeout=15)
    if kind == "error":
        assert writer.errors or writer.sent > writer.acked, f"error trial {trial}: writer saw no error after the fault"
    proc.kill()
    proc.wait(timeout=10)
    sent, acked = writer.sent, writer.acked
    device.recover()
    restarted = start_server(MNT / f"{kind}-{trial}" / "data")
    try:
        assert ping(), f"{kind} {trial}: no recovery"
        verify(sent, acked, f"{kind} trial {trial} (acked before fault {acked_before}, errors {writer.errors[:1]})")
    finally:
        restarted.kill()
        restarted.wait(timeout=10)


def main():
    which = next((a for a in sys.argv[1:] if not a.startswith("--")), "all")
    trials = int(os.environ.get("RUVIO_FAULT_TRIALS", "5"))
    random.seed(int(os.environ.get("RUVIO_FAULT_SEED", "1")))
    device = Device()
    try:
        for kind in (["error", "stall"] if which == "all" else [which]):
            for trial in range(1, trials + 1):
                scenario(device, kind, trial)
        print("DISK_FAULTS_DONE", flush=True)
    finally:
        device.close()


if __name__ == "__main__":
    main()
