#!/usr/bin/env python3
"""Sync worker off vs on with N shards (always durability). Linux, redis-benchmark. On a box with fewer than
shards+1 cores the client competes with the server, so read the ratio, not the absolute numbers.
Usage: bench_shards_ab.py <ruvio-binary> <shards> [rounds]"""
import os, shutil, socket, statistics, subprocess, sys, tempfile, time
BIN, SHARDS = sys.argv[1], int(sys.argv[2]); ROUNDS = int(sys.argv[3]) if len(sys.argv) > 3 else 5
TMP = os.environ.get("BENCH_TMP", "/root/ruvio-bench"); CPUS = os.environ.get("SERVER_CPUS", "0,1")
SHARD_CPUS = os.environ.get("SHARD_CPUS")  # RUVIO_CPUSET: shard i is pinned to the i-th entry
CLIENT_CPUS = os.environ.get("CLIENT_CPUS")
def ping(port):
    for _ in range(300):
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=1); s.sendall(b"*1\r\n$4\r\nPING\r\n"); ok = s.recv(8).startswith(b"+PONG"); s.close()
            if ok: return
        except OSError: pass
        time.sleep(0.05)
def bench(args, n, c, pl):
    b = (["taskset", "-c", CLIENT_CPUS] if CLIENT_CPUS else []) + ["redis-benchmark", "-p", "6731", "-d", "64", "-r", "100000", "-c", str(c), "-P", str(pl), "--csv", "-n", str(n), "--threads", "2"] + args
    out = subprocess.run(b, capture_output=True, text=True).stdout.splitlines()
    r = [l.replace('"', '').split(",") for l in out if l and not l.replace('"', '').startswith("test")][0]; return float(r[1]), float(r[6])
res = {m: {"c50": [], "p16": []} for m in ("off", "on")}
for r in range(ROUNDS):
    for mode in (["off", "on"] if r % 2 == 0 else ["on", "off"]):
        d = tempfile.mkdtemp(dir=TMP)
        env = dict(os.environ, RUVIO_ADDR="127.0.0.1:6731", RUVIO_DURABILITY="always", RUVIO_SHARDS=str(SHARDS), RUVIO_DATA_DIR=f"{d}/data", RUVIO_SYNC_WORKER=mode, **({"RUVIO_CPUSET": SHARD_CPUS} if SHARD_CPUS else {}))
        p = subprocess.Popen(["taskset", "-c", CPUS, BIN], env=env, cwd=d, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL); ping(6731)
        try:
            bench(["-t", "set"], 3000, 20, 1)
            res[mode]["c50"].append(bench(["-t", "set"], 40000, 50, 1))
            res[mode]["p16"].append(bench(["-t", "set"], 200000, 50, 16))
        finally:
            p.terminate(); p.wait(); shutil.rmtree(d, ignore_errors=True)
    print("round", r, flush=True)
for mode, v in res.items():
    for k, lst in v.items():
        print(f"shards={SHARDS} sync_worker={mode} {k}: {statistics.median(x[0] for x in lst):,.0f}/s p99 {statistics.median(x[1] for x in lst):.2f}ms", flush=True)
