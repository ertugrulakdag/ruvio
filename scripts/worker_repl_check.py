#!/usr/bin/env python3
"""Primary + replica, both with the sync worker on: replicated keys arrive, acknowledged keys survive kill -9.
Usage: scripts/worker_repl_check.py [binary]   (loopback only, ports 7401/7402/7501)"""
import os, signal, socket, subprocess, sys, tempfile, time
BIN = sys.argv[1] if len(sys.argv) > 1 else "target/release-fast/ruvio"
N = 3000

def cmd(sock, *parts):
    sock.sendall(b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n%s\r\n" % (len(p), p) for p in map(lambda x: x if isinstance(x, bytes) else str(x).encode(), parts)))
def reply(f):
    line = f.readline()
    if line[:1] == b"$":
        n = int(line[1:]);
        return None if n < 0 else (f.read(n + 2)[:-2])
    return line.rstrip()
def conn(port):
    for _ in range(200):
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=5); return s, s.makefile("rb")
        except OSError: time.sleep(0.05)
    raise SystemExit(f"port {port} never came up")
def start(d, name, port, role, mode="on", **extra):
    env = dict(os.environ, RUVIO_ADDR=f"127.0.0.1:{port}", RUVIO_DURABILITY="always", RUVIO_SHARDS="1",
               RUVIO_DATA_DIR=f"{d}/{name}", RUVIO_SYNC_WORKER=mode, RUVIO_REPLICATION_ROLE=role, **extra)
    return subprocess.Popen([BIN], env=env, stdout=open(f"{d}/{name}.log", "ab"), stderr=subprocess.STDOUT)

d = tempfile.mkdtemp()
pri = start(d, "p", 7401, "primary", RUVIO_REPLICATION_LISTEN="127.0.0.1:7501")
conn(7401)
rep = start(d, "r", 7402, "replica", RUVIO_REPLICATION_PRIMARY="127.0.0.1:7501")
conn(7402)
s, f = conn(7401)
for i in range(N):
    cmd(s, "SET", f"k{i}", f"v{i}")
    assert reply(f) == b"+OK", i
rs, rf = conn(7402)
ok = False
for _ in range(100):
    cmd(rs, "GET", f"k{N-1}")
    if reply(rf) == f"v{N-1}".encode(): ok = True; break
    time.sleep(0.1)
print("replica caught up:", ok)
for p in (pri, rep): p.send_signal(signal.SIGKILL); p.wait()
pri = start(d, "p", 7401, "off")
s, f = conn(7401)
lost = 0
for i in range(N):
    cmd(s, "GET", f"k{i}")
    if reply(f) != f"v{i}".encode(): lost += 1
print("acknowledged keys lost after kill -9 of primary:", lost)
rep = start(d, "r", 7402, "off")
s, f = conn(7402)
rl = 0
for i in range(N):
    cmd(s, "GET", f"k{i}")
    if reply(f) != f"v{i}".encode(): rl += 1
print("replica keys missing after kill -9 of replica:", rl)
for p in (pri, rep): p.kill()
sys.exit(0 if ok and lost == 0 else 1)
