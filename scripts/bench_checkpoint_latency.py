#!/usr/bin/env python3
"""GET latency on one connection while BGSAVE walks N keys (snapshot durability, 1 shard).
Usage: bench_checkpoint_latency.py <ruvio-binary> [keys]  (needs redis-benchmark and redis-cli)"""
import os, socket, subprocess, sys, tempfile, time
BIN = os.path.abspath(sys.argv[1]); KEYS = int(sys.argv[2]) if len(sys.argv) > 2 else 2_000_000
PORT = 6751
d = tempfile.mkdtemp()
env = dict(os.environ, RUVIO_ADDR=f"127.0.0.1:{PORT}", RUVIO_SHARDS="1", RUVIO_DURABILITY="snapshot", RUVIO_DATA_DIR=d + "/data",
           RUVIO_SNAPSHOT_INTERVAL_MS="3600000")
p = subprocess.Popen([BIN], env=env, cwd=d, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
def conn():
    for _ in range(100):
        try: return socket.create_connection(("127.0.0.1", PORT))
        except OSError: time.sleep(0.1)
s = conn()
subprocess.run(["redis-benchmark", "-p", str(PORT), "-t", "set", "-n", str(KEYS), "-r", str(KEYS), "-d", "64", "-c", "50", "-P", "32", "-q"], capture_output=True)
def cmd(*parts):
    s.sendall(b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in map(str.encode, parts))); return s.recv(65536)
def probe(secs):
    xs = []; end = time.time() + secs
    while time.time() < end:
        t = time.perf_counter(); cmd("GET", "key:000000000001"); xs.append((time.perf_counter() - t) * 1e3)
    xs.sort(); return f"n={len(xs)} p50 {xs[len(xs)//2]:.3f} p99 {xs[int(len(xs)*.99)]:.3f} p99.9 {xs[int(len(xs)*.999)]:.3f} max {xs[-1]:.3f} ms"
print("keys:", cmd("DBSIZE").strip().decode())
print("idle      :", probe(3))
cmd("BGSAVE"); t0 = time.time()
print("checkpoint:", probe(6))
info = cmd("INFO", "persistence").decode()
print("seconds since BGSAVE:", f"{time.time()-t0:.1f}")
print("\n".join(l for l in info.splitlines() if "checkpoint" in l.lower() or "save" in l.lower()))
print("checkpoint files:", sorted(os.listdir(d + "/data"))[:6], subprocess.run(["du", "-sh", d + "/data"], capture_output=True, text=True).stdout.split()[0])
p.terminate()
