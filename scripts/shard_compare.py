#!/usr/bin/env python3
"""The command comparison of redis_compare_features.py on a whole machine: Ruvio with N shards
(one process, `port_mode = "per-shard"`) against one Redis process on one core and against a
Redis Cluster of N masters on N cores.

Every system gets the same work per shard: one `redis-benchmark` per shard, pinned to the client
cores, sends the row's command to that shard's port with the row's keys wrapped in a hash tag
that the shard owns (`{tag}key`), so no command crosses shards and the slot map is never a
factor. One Redis process is one shard. Rates are summed over the shards; the table also shows
the rate per server core. The memory table loads the same structures into each system, routed
by slot, and compares the resident memory of all its processes.

    python3 scripts/shard_compare.py --ruvio ~/ruvio-new --redis-server ~/redis-server \
        --redis-cli ~/redis-cli --redis-benchmark ~/redis-benchmark --shards 8 \
        --server-cpus 8-15 --client-cpus 0-7 --out ~/shard-compare

Linux only (taskset). Writes results.json and table.md into --out.
"""

from __future__ import annotations

import argparse
import json
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import redis_compare as rc  # noqa: E402
from redis_compare_features import FEATURES, MEMORY_CASES, RUVIO_ONLY, SETUP, frame, memory_frames  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
SLOTS = 16384
SYSTEMS = ("ruvio", "redis1", "rediscl")
# Every argument of SETUP, FEATURES and RUVIO_ONLY that names a key or channel.
KEYS = {
    "lst", "k", "k2", "sk", "bm", "bf", "ctr", "ttlk", "fl", "h", "s", "z", "lst2", "hm", "lm",
    "hll", "geo", "nokey", "ap", "lp", "ch", "ma", "mb", "mc", "fillkey",
}


def crc16(data: bytes) -> int:
    crc = 0

    for byte in data:
        crc ^= byte << 8

        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF

    return crc


def hash_slot(key: str) -> int:
    start = key.find("{")

    if start >= 0:
        end = key.find("}", start + 1)

        if end > start + 1:
            key = key[start + 1:end]

    return crc16(key.encode()) % SLOTS


def tagged(argv: list[str], tag: str) -> list[str]:
    return [f"{{{tag}}}{a}" if a in KEYS or a.startswith("ex:") else a for a in argv]


def cpu_list(spec: str) -> list[int]:
    cpus: list[int] = []

    for part in spec.split(","):
        if "-" in part:
            low, high = part.split("-")
            cpus.extend(range(int(low), int(high) + 1))
        else:
            cpus.append(int(part))

    return cpus


def send_all(port: int, frames: list[bytes], batch: int = 200) -> None:
    with socket.create_connection(("127.0.0.1", port)) as sock:
        for first in range(0, len(frames), batch):
            sock.sendall(b"".join(frames[first:first + batch]) + frame("PING"))
            received = bytearray()

            while not received.endswith(b"+PONG\r\n"):
                chunk = sock.recv(1 << 16)

                if not chunk:
                    raise ConnectionError("server closed during load")

                received.extend(chunk)

            if b"\r\n-" in b"\r\n" + bytes(received):
                raise RuntimeError(f"error reply during load: {bytes(received)[:200]!r}")


def reply(port: int, *parts: str) -> bytes:
    return rc.command(port, *[p.encode() for p in parts])


def parse_resp(data: bytes, pos: int):
    """One RESP2 value at `pos`: (value, next position). Bulk strings come back as str."""
    end = data.index(b"\r\n", pos)
    line, pos = data[pos:end], end + 2
    kind, body = line[:1], line[1:]

    if kind == b":":
        return int(body), pos
    if kind in (b"+", b"-"):
        return body.decode(), pos
    if kind == b"$":
        length = int(body)

        if length < 0:
            return None, pos

        return data[pos:pos + length].decode(), pos + length + 2

    items = []

    for _ in range(int(body)):
        item, pos = parse_resp(data, pos)
        items.append(item)

    return items, pos


class System:
    """One system under test: its processes, its shard ports and which slots each port owns."""

    def __init__(self, name: str, procs: list[subprocess.Popen], ports: list[int]):
        self.name = name
        self.procs = procs
        self.ports = ports
        self.owner: list[int] = []
        self.tags: dict[int, str] = {}

    def discover(self) -> None:
        """Slot owners from `CLUSTER SLOTS` (one port owns everything on a single node)."""
        if len(self.ports) == 1:
            self.owner = [self.ports[0]] * SLOTS
        else:
            self.owner = [0] * SLOTS
            value, _ = parse_resp(reply(self.ports[0], "CLUSTER", "SLOTS"), 0)

            for entry in value:
                low, high, node = entry[0], entry[1], entry[2]
                port = node[1]

                for slot in range(low, high + 1):
                    self.owner[slot] = port

            if any(port == 0 for port in self.owner):
                raise RuntimeError(f"{self.name}: CLUSTER SLOTS left slots unowned")

        for port in self.ports:
            self.tags[port] = next(f"t{n}" for n in range(100_000) if self.owner[hash_slot(f"t{n}")] == port)

    def rss_kib(self) -> int:
        total = 0

        for proc in self.procs:
            for line in Path(f"/proc/{proc.pid}/status").read_text().splitlines():
                if line.startswith("VmRSS:"):
                    total += int(line.split()[1])

        return total

    def stop(self) -> None:
        for proc in self.procs:
            if proc.poll() is None:
                proc.terminate()

        for proc in self.procs:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def pinned(cpus: str, argv: list[str]) -> list[str]:
    return ["taskset", "-c", cpus, *argv]


def redis_persistence(args: argparse.Namespace) -> list[str]:
    if args.durability in ("everysec", "always"):
        return ["--save", "", "--appendonly", "yes", "--appendfsync", args.durability]

    return ["--save", "", "--appendonly", "no"]


def start(args: argparse.Namespace, name: str, workdir: Path) -> System:
    directory = Path(tempfile.mkdtemp(prefix=f"{name}-", dir=workdir))
    server_cpus = cpu_list(args.server_cpus)
    n = args.shards
    aof = redis_persistence(args)

    if name == "ruvio":
        argv = [
            str(args.ruvio), "--address", f"127.0.0.1:{args.ruvio_port}", "--durability", args.durability,
            "--shards", str(n), "--cpu-set", args.server_cpus, "--data-dir", str(directory / "data"),
        ]
        proc = subprocess.Popen(argv, cwd=directory, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        ports = [args.ruvio_port + i for i in range(n)]
        system = System(name, [proc], ports)
    elif name == "redis1":
        argv = pinned(str(server_cpus[0]), [
            args.redis_server, "--bind", "127.0.0.1", "--port", str(args.redis1_port), *aof, "--dir", str(directory),
        ])
        proc = subprocess.Popen(argv, cwd=directory, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        system = System(name, [proc], [args.redis1_port])
    else:
        procs = []
        ports = [args.rediscl_port + i for i in range(n)]

        for i, port in enumerate(ports):
            node_dir = directory / f"node{i}"
            node_dir.mkdir()
            argv = pinned(str(server_cpus[i % len(server_cpus)]), [
                args.redis_server, "--bind", "127.0.0.1", "--port", str(port), *aof,
                "--dir", str(node_dir), "--cluster-enabled", "yes", "--cluster-config-file", f"nodes-{port}.conf",
            ])
            procs.append(subprocess.Popen(argv, cwd=node_dir, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))

        system = System(name, procs, ports)

    for port in system.ports:
        rc.wait_ping(port)

    if name == "rediscl":
        subprocess.run(
            [args.redis_cli, "--cluster", "create", *[f"127.0.0.1:{p}" for p in system.ports], "--cluster-replicas", "0", "--cluster-yes"],
            check=True, capture_output=True, text=True,
        )

        for _ in range(100):
            if b"cluster_state:ok" in reply(system.ports[0], "CLUSTER", "INFO"):
                break
            time.sleep(0.2)
        else:
            raise RuntimeError("redis cluster did not reach cluster_state:ok")

    system.discover()
    return system


def bench(args: argparse.Namespace, system: System, argv: list[str], requests: int) -> float:
    """One redis-benchmark per shard, in parallel; the row's keys are tagged for that shard."""
    client_cpus = cpu_list(args.client_cpus)
    procs = []

    for i, port in enumerate(system.ports):
        cmd = pinned(str(client_cpus[i % len(client_cpus)]), [
            args.redis_benchmark, "-h", "127.0.0.1", "-p", str(port), "-n", str(requests), "-c", str(args.clients),
            "-P", str(args.pipeline), "-r", "1000", "--csv", *tagged(argv, system.tags[port]),
        ])
        procs.append(subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))

    total = 0.0

    for proc in procs:
        out, err = proc.communicate()

        if proc.returncode != 0:
            raise RuntimeError(f"redis-benchmark failed: {err[:120]!r}")

        for line in out.splitlines():
            fields = [f.strip('"') for f in line.split(",")]

            if len(fields) >= 2 and fields[0] != "test":
                total += float(fields[1])
                break
        else:
            raise RuntimeError(f"no rate for {argv}: {out!r}")

    return total


def setup(system: System) -> None:
    for port in system.ports:
        tag = system.tags[port]
        send_all(port, [frame(*tagged(list(parts), tag)) for parts in SETUP])
        send_all(port, [frame("PFADD", f"{{{tag}}}hll", *[f"s{i}" for i in range(1000)])])
        send_all(port, [frame("GEOADD", f"{{{tag}}}geo", "13.361389", "38.115556", "seed")])

        if system.name == "ruvio":
            token = reply(port, "FILLGET", f"{{{tag}}}fillkey", "5000").split(b"\r\n")[-2].decode()
            reply(port, "FILLSET", f"{{{tag}}}fillkey", "v", "60000", token)
        else:
            reply(port, "SET", f"{{{tag}}}fillkey", "v")


def key_of(frame_bytes: bytes) -> str:
    # *N\r\n$len\r\nCMD\r\n$len\r\nKEY\r\n...
    parts = frame_bytes.split(b"\r\n")
    return parts[4].decode()


def load_routed(system: System, frames: list[bytes]) -> None:
    by_port: dict[int, list[bytes]] = {port: [] for port in system.ports}

    for f in frames:
        by_port[system.owner[hash_slot(key_of(f))]].append(f)

    for port, group in by_port.items():
        if group:
            send_all(port, group)


def copies(frames: list[bytes], count: int) -> list[bytes]:
    """`count` copies of the structures with distinct keys (`c3-l:12`), so an N-shard system
    holds the same data per shard as the single process holds in total."""
    out = []

    for c in range(count):
        for f in frames:
            parts = f.split(b"\r\n")
            key = b"c%d-" % c + parts[4]
            parts[3] = b"$%d" % len(key)
            parts[4] = key
            out.append(b"\r\n".join(parts))

    return out


def measure_memory(args: argparse.Namespace, name: str, workdir: Path, kind: str) -> dict:
    system = start(args, name, workdir)
    count = 1 if name == "redis1" else args.shards

    try:
        time.sleep(0.5)
        idle = system.rss_kib()
        load_routed(system, copies(memory_frames(kind), count))
        time.sleep(0.5)
        loaded = system.rss_kib()
    finally:
        system.stop()

    return {"idle_kib": idle, "loaded_kib": loaded, "delta_kib": loaded - idle, "copies": count}


def rate(value: float | None) -> str:
    return "n/a" if value is None or value != value else f"{value / 1e6:.2f}M/s" if value >= 1e6 else f"{value / 1e3:.0f}k/s"


def render(results: dict) -> str:
    n = results["config"]["shards"]
    lines = [
        f"| Command | Ruvio {n} shards | Redis 1 core | Redis Cluster {n} | Ruvio / Redis 1 core (per core) | Ruvio / Redis Cluster | Note |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]

    for row in results["rows"]:
        ruvio, r1, rcl = row.get("ruvio"), row.get("redis1"), row.get("rediscl")
        per_core = f"{(ruvio / n) / r1:.2f}×" if r1 and ruvio else ""
        vs_cluster = f"{ruvio / rcl:.2f}×" if rcl and ruvio else ""
        lines.append(f"| {row['label']} | {rate(ruvio)} | {rate(r1)} | {rate(rcl)} | {per_core} | {vs_cluster} | {row['note']} |")

    lines += [
        "",
        f"| Structure (per shard; the {n}-shard systems hold {n} copies) | Ruvio {n} shards, total | Redis 1 core | Redis Cluster {n}, total | Ruvio per shard / Redis 1 | Ruvio / Cluster |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]

    for row in results["memory"]:
        d = {name: row[name]["delta_kib"] for name in SYSTEMS}
        per_shard = d.get("ruvio", 0) / row["ruvio"]["copies"] if "ruvio" in d else 0
        cell = lambda name: f"{d[name] / 1024:.1f} MiB" if name in d else "n/a"
        vs1 = f"{per_shard / d['redis1']:.2f}×" if "redis1" in d and per_shard else ""
        vscl = f"{d['ruvio'] / d['rediscl']:.2f}×" if "rediscl" in d and "ruvio" in d else ""
        lines.append(f"| {row['label']} | {cell('ruvio')} | {cell('redis1')} | {cell('rediscl')} | {vs1} | {vscl} |")

    return "\n".join(lines) + "\n"


def main() -> None:
    global SYSTEMS
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ruvio", type=Path, required=True)
    parser.add_argument("--redis-server", default=shutil.which("redis-server") or "redis-server")
    parser.add_argument("--redis-cli", default=shutil.which("redis-cli") or "redis-cli")
    parser.add_argument("--redis-benchmark", default=shutil.which("redis-benchmark") or "redis-benchmark")
    parser.add_argument("--shards", type=int, default=8)
    parser.add_argument("--server-cpus", default="8-15", help="CPU list for the shards / Redis processes")
    parser.add_argument("--client-cpus", default="0-7", help="CPU list for the redis-benchmark processes")
    parser.add_argument("--durability", default="memory", help="Ruvio durability profile; with everysec/always Redis gets appendonly yes, appendfsync <same>")
    parser.add_argument("--data-root", type=Path, default=None, help="directory for server data (a real disk for everysec/always); default: a temp dir")
    parser.add_argument("--ruvio-port", type=int, default=7000)
    parser.add_argument("--redis1-port", type=int, default=7100)
    parser.add_argument("--rediscl-port", type=int, default=7200)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--clients", type=int, default=50)
    parser.add_argument("--pipeline", type=int, default=16)
    parser.add_argument("--scale", type=float, default=1.0, help="multiply every per-shard request count")
    parser.add_argument("--no-memory", action="store_true")
    parser.add_argument("--only", default="", help="run only rows whose label contains this text")
    parser.add_argument("--systems", default=",".join(SYSTEMS), help="comma list of ruvio, redis1, rediscl (a Redis Cluster needs at least 3 shards)")
    parser.add_argument("--out", type=Path, default=ROOT / "target/shard-compare")
    args = parser.parse_args()
    SYSTEMS = tuple(args.systems.split(","))

    args.out.mkdir(parents=True, exist_ok=True)
    workdir = Path(tempfile.mkdtemp(prefix="shard-compare-", dir=args.data_root))
    started = time.monotonic()
    systems = {name: start(args, name, workdir) for name in SYSTEMS}
    rows = []

    try:
        for system in systems.values():
            setup(system)

        plan = [(label, argv, argv, reqs, note) for label, argv, reqs, note in FEATURES]
        plan += [(label, ruvio_argv, redis_argv, 300_000, note) for label, ruvio_argv, redis_argv, note in RUVIO_ONLY]
        plan = [row for row in plan if args.only in row[0]]

        for label, ruvio_argv, redis_argv, requests, note in plan:
            requests = max(10_000, int(requests * args.scale))
            samples: dict[str, list[float]] = {name: [] for name in SYSTEMS}

            for round_index in range(args.rounds):
                order = list(SYSTEMS) if round_index % 2 == 0 else list(reversed(SYSTEMS))

                for name in order:
                    argv = ruvio_argv if name == "ruvio" else redis_argv

                    try:
                        samples[name].append(bench(args, systems[name], argv, requests))
                    except RuntimeError as error:
                        samples[name].append(float("nan"))
                        note = note or f"{name}: {str(error)[:60]}"

            medians = {name: statistics.median(samples[name]) for name in SYSTEMS}
            rows.append({"label": label, **{name: (None if v != v else v) for name, v in medians.items()}, "note": note, "samples": samples})
            print(f"{label:<42} " + "  ".join(f"{name} {medians[name]:>12,.0f}" for name in SYSTEMS), flush=True)
    finally:
        for system in systems.values():
            system.stop()

    memory = []

    if not args.no_memory:
        for label, kind in MEMORY_CASES:
            result = {name: measure_memory(args, name, workdir, kind) for name in SYSTEMS}
            memory.append({"label": label, **result})
            print(f"{label:<42} " + "  ".join(f"{name} {result[name]['delta_kib'] / 1024:>7.1f} MiB" for name in SYSTEMS), flush=True)

    shutil.rmtree(workdir, ignore_errors=True)
    results = {
        "date": time.strftime("%Y-%m-%d"),
        "host": rc.host_info(),
        "build": rc.git_build(),
        "redis_version": rc.version([args.redis_server, "--version"]),
        "config": {
            "shards": args.shards, "rounds": args.rounds, "clients": args.clients, "pipeline": args.pipeline,
            "scale": args.scale, "durability": args.durability, "server_cpus": args.server_cpus, "client_cpus": args.client_cpus,
            "method": "one redis-benchmark per shard on its own port with keys in a hash tag that shard owns; rates summed over shards",
        },
        "rows": rows,
        "memory": memory,
        "seconds": round(time.monotonic() - started, 1),
    }
    markdown = render(results)
    (args.out / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    (args.out / "table.md").write_text(markdown)
    print()
    print(markdown)
    print(f"{results['seconds']} s, written to {args.out}")


if __name__ == "__main__":
    main()
