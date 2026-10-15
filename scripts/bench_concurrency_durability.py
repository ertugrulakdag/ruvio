#!/usr/bin/env python3
"""Concurrency, durability and multi-shard scenarios, Ruvio vs Redis (Linux, native processes, pinned CPUs).

    python3 scripts/bench_concurrency_durability.py --ruvio target/release/ruvio --redis-server REDIS --redis-benchmark BENCH \
        --data-dir /mnt/data --server-cpus 0 --client-cpus 2,4 --rounds 5 --out result.json

Each scenario alternates which server runs first; medians are reported. `always` durability scenarios use --data-dir
(put it on the disk you want to measure). Multi-shard scenarios pin shard i to the i-th CPU of --shard-cpus and the client to --multi-client-cpus.
"""
import argparse, json, os, shutil, socket, statistics, subprocess, tempfile, time

ap = argparse.ArgumentParser()
ap.add_argument("--ruvio", required=True); ap.add_argument("--redis-server", required=True); ap.add_argument("--redis-benchmark", required=True)
ap.add_argument("--data-dir", default="/tmp"); ap.add_argument("--server-cpus", default="0"); ap.add_argument("--client-cpus", default="2,4")
ap.add_argument("--shard-cpus", default="0,2,4,6"); ap.add_argument("--multi-client-cpus", default="1,3,5,7"); ap.add_argument("--client-threads", default="2")
ap.add_argument("--rounds", type=int, default=5); ap.add_argument("--out", default="concurrency-durability.json")
ap.add_argument("--only", default="", help="substring filter on scenario labels")
a = ap.parse_args()
PORT = {"ruvio": 6441, "redis": 6442}


def ping(port):
    for _ in range(200):
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=1); s.sendall(b"*1\r\n$4\r\nPING\r\n"); ok = s.recv(8).startswith(b"+PONG"); s.close()
            if ok: return
        except OSError: pass
        time.sleep(0.05)
    raise SystemExit("server did not start")


class Srv:
    def __init__(self, kind, durable, shards=1, cpus=None):
        self.kind = kind; self.dir = tempfile.mkdtemp(dir=a.data_dir); port = PORT[kind]; self.port = port
        pin = ["taskset", "-c", cpus or a.server_cpus]
        env = dict(os.environ)
        if kind == "ruvio":
            argv = [a.ruvio, "--address", f"127.0.0.1:{port}", "--durability", "always" if durable else "memory", "--shards", str(shards), "--data-dir", self.dir + "/data"]
            if shards > 1: env["RUVIO_CPUSET"] = (cpus or a.server_cpus)
            else: env["RUVIO_CORE"] = (cpus or a.server_cpus).split(",")[0]
        else:
            argv = [a.redis_server, "--bind", "127.0.0.1", "--port", str(port), "--save", "", "--dir", self.dir] + (["--appendonly", "yes", "--appendfsync", "always"] if durable else ["--appendonly", "no"])
        self.p = subprocess.Popen(pin + argv, cwd=self.dir, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL); ping(port)

    def stop(self):
        self.p.terminate(); self.p.wait(); shutil.rmtree(self.dir, ignore_errors=True)


def bench(port, test_args, n, c, p, cpus):
    cmd = ["taskset", "-c", cpus, a.redis_benchmark, "-h", "127.0.0.1", "-p", str(port), "-n", str(n), "-c", str(c), "-P", str(p), "-r", "100000", "--threads", a.client_threads, "--csv"] + test_args
    out = subprocess.run(cmd, capture_output=True, text=True).stdout.splitlines()
    r = [l.replace('"', "").split(",") for l in out if l and not l.replace('"', "").startswith("test")][0]
    return float(r[1]), float(r[6]) if len(r) > 6 else float("nan")


SET64, GET64 = ["-t", "set", "-d", "64"], ["-t", "get", "-d", "64"]
# label, durable, args, n, clients, pipeline, ruvio shards (1 = the single-thread comparison), shard cpus
SCEN = [
    ("SET, 50 clients, no pipeline", False, SET64, 600_000, 50, 1, 1),
    ("GET, 50 clients, no pipeline", False, GET64, 600_000, 50, 1, 1),
    ("SET, 200 clients, no pipeline", False, SET64, 600_000, 200, 1, 1),
    ("GET, 200 clients, no pipeline", False, GET64, 600_000, 200, 1, 1),
    ("SET, 200 clients, pipeline 16", False, SET64, 2_000_000, 200, 16, 1),
    ("HSET, 200 clients, pipeline 16", False, ["HSET", "h:__rand_int__", "f", "v"], 2_000_000, 200, 16, 1),
    ("SET, durability always (fsync), 50 clients", True, SET64, 60_000, 50, 1, 1),
    ("SET, durability always (fsync), 200 clients", True, SET64, 100_000, 200, 1, 1),
    ("SET, durability always (fsync), 50 clients, pipeline 16", True, SET64, 300_000, 50, 16, 1),
    ("SET, 200 clients, pipeline 16, Ruvio 2 shards", False, SET64, 2_000_000, 200, 16, 2),
    ("SET, 200 clients, pipeline 16, Ruvio 4 shards", False, SET64, 2_000_000, 200, 16, 4),
    ("GET, 200 clients, pipeline 16, Ruvio 2 shards", False, GET64, 2_000_000, 200, 16, 2),
    ("SET, durability always, 200 clients, pipeline 16, Ruvio 2 shards", True, SET64, 300_000, 200, 16, 2),
]
res = []
for label, durable, args, n, c, p, shards in SCEN:
    if a.only and a.only not in label: continue
    cpus = ",".join(a.shard_cpus.split(",")[:shards]) if shards > 1 else None
    client = a.multi_client_cpus if shards > 1 else a.client_cpus
    samples = {"ruvio": [], "redis": []}
    for rnd in range(a.rounds):
        for kind in (("ruvio", "redis") if rnd % 2 == 0 else ("redis", "ruvio")):
            s = Srv(kind, durable, shards if kind == "ruvio" else 1, cpus if kind == "ruvio" else None)
            try:
                bench(s.port, args, max(n // 10, 5000), c, p, client)
                samples[kind].append(bench(s.port, args, n, c, p, client))
            finally:
                s.stop()
    row = {"label": label, "ruvio_shards": shards}
    for kind, v in samples.items():
        row[kind] = {"rps": statistics.median(x[0] for x in v), "p99_ms": statistics.median(x[1] for x in v), "samples": [x[0] for x in v]}
    res.append(row)
    print(f"{label:68} ruvio {row['ruvio']['rps']:>11,.0f} p99 {row['ruvio']['p99_ms']:.2f}   redis {row['redis']['rps']:>11,.0f} p99 {row['redis']['p99_ms']:.2f}   {row['ruvio']['rps']/row['redis']['rps']:.2f}x", flush=True)
    json.dump(res, open(a.out, "w"), indent=1)
