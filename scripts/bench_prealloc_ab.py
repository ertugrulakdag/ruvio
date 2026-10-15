#!/usr/bin/env python3
"""A/B two ruvio binaries (always durability, 1 shard) with the sync worker off and on. Linux, redis-benchmark.
Usage: bench_prealloc_ab.py <old-binary> <new-binary> [rounds]"""
import os, shutil, socket, statistics, subprocess, sys, tempfile, time
OLD, NEW = sys.argv[1], sys.argv[2]; ROUNDS = int(sys.argv[3]) if len(sys.argv) > 3 else 5
TMP = os.environ.get("BENCH_TMP", "/root/ruvio-bench")
def ping(port):
    for _ in range(300):
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=1); s.sendall(b"*1\r\n$4\r\nPING\r\n"); ok = s.recv(8).startswith(b"+PONG"); s.close()
            if ok: return
        except OSError: pass
        time.sleep(0.05)
def bench(args, n, c, pl):
    b = ["taskset", "-c", "0", "redis-benchmark", "-p", "6711", "-d", "64", "-r", "100000", "-c", str(c), "-P", str(pl), "--csv", "-n", str(n)] + args
    out = subprocess.run(b, capture_output=True, text=True).stdout.splitlines()
    r = [l.replace('"', '').split(",") for l in out if l and not l.replace('"', '').startswith("test")][0]; return float(r[1]), float(r[6])
cases = [(b, m) for b in ("old", "new") for m in ("off", "on")]
res = {c: {"c50": [], "p16": []} for c in cases}
for r in range(ROUNDS):
    order = cases[r % 4:] + cases[:r % 4]
    for binary, mode in order:
        d = tempfile.mkdtemp(dir=TMP)
        env = dict(os.environ, RUVIO_ADDR="127.0.0.1:6711", RUVIO_DURABILITY="always", RUVIO_SHARDS="1", RUVIO_DATA_DIR=f"{d}/data", RUVIO_SYNC_WORKER=mode)
        p = subprocess.Popen(["taskset", "-c", "1", OLD if binary == "old" else NEW], env=env, cwd=d, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL); ping(6711)
        try:
            bench(["-t", "set"], 3000, 20, 1)
            res[(binary, mode)]["c50"].append(bench(["-t", "set"], 40000, 50, 1))
            res[(binary, mode)]["p16"].append(bench(["-t", "set"], 200000, 50, 16))
        finally:
            p.terminate(); p.wait(); shutil.rmtree(d, ignore_errors=True)
    print("round", r, flush=True)
for (binary, mode), v in res.items():
    for k, lst in v.items():
        print(f"{binary} sync_worker={mode} {k}: {statistics.median(x[0] for x in lst):,.0f}/s p99 {statistics.median(x[1] for x in lst):.2f}ms", flush=True)
