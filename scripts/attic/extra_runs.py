#!/usr/bin/env python3
"""Concurrency and durability scenarios, Ruvio vs Redis 8.10.2. Server cpu 1, client cpu 0.
    python3 extra_runs.py RUVIO_BIN REDIS_SERVER [--docker RUVIO_IMAGE REDIS_IMAGE]"""
import json, os, shutil, socket, statistics, subprocess, sys, tempfile, time
RUVIO, REDIS = sys.argv[1], sys.argv[2]
DOCKER = None
if "--docker" in sys.argv:
    i = sys.argv.index("--docker"); DOCKER = (sys.argv[i + 1], sys.argv[i + 2])
RR, DR = 6441, 6442
def ping(port):
    for _ in range(200):
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=1); s.sendall(b"*1\r\n$4\r\nPING\r\n"); ok = s.recv(8).startswith(b"+PONG"); s.close()
            if ok: return
        except OSError: pass
        time.sleep(0.05)
    raise SystemExit("no PONG")
class Srv:
    def __init__(self, kind, durable):
        self.kind = kind; self.dir = tempfile.mkdtemp(); port = RR if kind == "ruvio" else DR; self.port = port
        if DOCKER:
            self.name = f"x-{kind}"; subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)
            base = ["docker", "run", "-d", "--rm", "--network", "host", "--cpuset-cpus", "1", "--name", self.name, "-v", f"{self.dir}:/data"]
            if kind == "ruvio":
                cmd = base + ["--user", "0", DOCKER[0], "--address", f"127.0.0.1:{port}", "--durability", "always" if durable else "memory", "--shards", "1", "--data-dir", "/data"]
            else:
                cmd = base + [DOCKER[1], "redis-server", "--bind", "127.0.0.1", "--port", str(port), "--save", "", "--dir", "/data"] + (["--appendonly", "yes", "--appendfsync", "always"] if durable else ["--appendonly", "no"])
            subprocess.run(cmd, check=True, capture_output=True)
        else:
            if kind == "ruvio":
                argv = [RUVIO, "--address", f"127.0.0.1:{port}", "--durability", "always" if durable else "memory", "--shards", "1", "--data-dir", self.dir + "/data"]
            else:
                argv = [REDIS, "--bind", "127.0.0.1", "--port", str(port), "--save", "", "--dir", self.dir] + (["--appendonly", "yes", "--appendfsync", "always"] if durable else ["--appendonly", "no"])
            self.p = subprocess.Popen(["taskset", "-c", "1"] + argv, cwd=self.dir, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        ping(port)
    def stop(self):
        if DOCKER: subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)
        else: self.p.terminate(); self.p.wait()
        shutil.rmtree(self.dir, ignore_errors=True)
def bench(port, test_args, n, c, p):
    cmd = ["taskset", "-c", "0", "redis-benchmark", "-h", "127.0.0.1", "-p", str(port), "-n", str(n), "-c", str(c), "-P", str(p), "-r", "100000", "--csv"] + test_args
    out = subprocess.run(cmd, capture_output=True, text=True).stdout.splitlines()
    rows = [l.replace('"', "").split(",") for l in out if l and not l.startswith("test")]
    r = rows[0]
    return float(r[1]), float(r[6]) if len(r) > 6 else float("nan")  # rps, p99 ms
SCEN = [  # label, durable, redis-benchmark args, n, clients, pipeline
    ("SET, 50 clients, no pipeline", False, ["-t", "set", "-d", "64"], 400_000, 50, 1),
    ("GET, 50 clients, no pipeline", False, ["-t", "get", "-d", "64"], 400_000, 50, 1),
    ("SET, 200 clients, no pipeline", False, ["-t", "set", "-d", "64"], 400_000, 200, 1),
    ("GET, 200 clients, no pipeline", False, ["-t", "get", "-d", "64"], 400_000, 200, 1),
    ("SET, 200 clients, pipeline 16", False, ["-t", "set", "-d", "64"], 800_000, 200, 16),
    ("HSET, 200 clients, pipeline 16", False, ["HSET", "h:__rand_int__", "f", "v"], 800_000, 200, 16),
    ("SET, durability always (fsync), 50 clients", True, ["-t", "set", "-d", "64"], 30_000, 50, 1),
    ("SET, durability always (fsync), 50 clients, pipeline 16", True, ["-t", "set", "-d", "64"], 100_000, 50, 16),
]
res = []
for label, durable, args, n, c, p in SCEN:
    samples = {"ruvio": [], "redis": []}
    for rnd in range(3):
        for kind in (("ruvio", "redis") if rnd % 2 == 0 else ("redis", "ruvio")):
            s = Srv(kind, durable)
            try:
                bench(s.port, args, max(n // 10, 5000), c, p)  # warm up
                samples[kind].append(bench(s.port, args, n, c, p))
            finally: s.stop()
    row = {"label": label}
    for kind in samples:
        row[kind] = {"rps": statistics.median(x[0] for x in samples[kind]), "p99_ms": statistics.median(x[1] for x in samples[kind])}
    res.append(row)
    print(f"{label:58} ruvio {row['ruvio']['rps']:>10,.0f} p99 {row['ruvio']['p99_ms']:.2f} ms   redis {row['redis']['rps']:>10,.0f} p99 {row['redis']['p99_ms']:.2f} ms   ratio {row['ruvio']['rps']/row['redis']['rps']:.2f}x", flush=True)
json.dump(res, open("extra-docker.json" if DOCKER else "extra.json", "w"), indent=1)
