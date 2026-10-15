#!/usr/bin/env python3
"""SET throughput with a replica attached: sync worker off vs on (primary and replica alike). Linux, 2+ cores.
Usage: bench_worker_repl.py <ruvio-binary> [rounds]"""
import os, shutil, socket, statistics, subprocess, sys, tempfile, time
BIN = sys.argv[1]; ROUNDS = int(sys.argv[2]) if len(sys.argv) > 2 else 5
def ping(port):
    for _ in range(300):
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=1); s.sendall(b"*1\r\n$4\r\nPING\r\n"); ok = s.recv(8).startswith(b"+PONG"); s.close()
            if ok: return
        except OSError: pass
        time.sleep(0.05)
def start(d, name, port, role, mode, cpu, **extra):
    env = dict(os.environ, RUVIO_ADDR=f"127.0.0.1:{port}", RUVIO_DURABILITY="always", RUVIO_SHARDS="1", RUVIO_DATA_DIR=f"{d}/{name}",
               RUVIO_SYNC_WORKER=mode, RUVIO_REPLICATION_ROLE=role, **extra)
    p = subprocess.Popen(["taskset", "-c", cpu, BIN], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL); ping(port); return p
def bench(args, n, c, pl):
    b = ["taskset", "-c", "0", "redis-benchmark", "-p", "6701", "-d", "64", "-r", "100000", "-c", str(c), "-P", str(pl), "--csv", "-n", str(n)] + args
    out = subprocess.run(b, capture_output=True, text=True).stdout.splitlines()
    r = [l.replace('"', '').split(",") for l in out if l and not l.replace('"', '').startswith("test")][0]; return float(r[1]), float(r[6])
res = {"off": {"c50": [], "p16": []}, "on": {"c50": [], "p16": []}}
for r in range(ROUNDS):
    for mode in (["off", "on"] if r % 2 == 0 else ["on", "off"]):
        d = tempfile.mkdtemp(dir=os.environ.get("BENCH_TMP", "/root/ruvio-bench"))
        pri = start(d, "p", 6701, "primary", mode, "1", RUVIO_REPLICATION_LISTEN="127.0.0.1:6801")
        rep = start(d, "r", 6702, "replica", mode, "0", RUVIO_REPLICATION_PRIMARY="127.0.0.1:6801")
        try:
            bench(["-t", "set"], 3000, 20, 1)
            res[mode]["c50"].append(bench(["-t", "set"], 40000, 50, 1))
            res[mode]["p16"].append(bench(["-t", "set"], 200000, 50, 16))
        finally:
            for p in (pri, rep): p.terminate(); p.wait()
            shutil.rmtree(d, ignore_errors=True)
    print("round", r, flush=True)
for mode, v in res.items():
    for k, lst in v.items():
        print(f"replica attached, sync_worker={mode} {k}: {statistics.median(x[0] for x in lst):,.0f}/s p99 {statistics.median(x[1] for x in lst):.2f}ms", flush=True)
