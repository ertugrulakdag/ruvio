import os, signal, socket, subprocess, sys, tempfile, threading, time, random
BIN = sys.argv[1]; SH = sys.argv[2]; MODE = sys.argv[3]; TRIALS = int(sys.argv[4]); PORT = 6841
def enc(*a): return b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in (y if isinstance(y, bytes) else str(y).encode() for y in a))
def start(d):
    env = dict(os.environ, RUVIO_ADDR=f"127.0.0.1:{PORT}", RUVIO_SHARDS=SH, RUVIO_DURABILITY="always", RUVIO_DATA_DIR=d + "/data", RUVIO_SYNC_WORKER=MODE)
    p = subprocess.Popen([BIN], env=env, cwd=d, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        try: socket.create_connection(("127.0.0.1", PORT), timeout=1).close(); return p
        except OSError: time.sleep(0.1)
    raise SystemExit("no start")
bad = 0
for trial in range(TRIALS):
    d = tempfile.mkdtemp(dir="/mnt/data"); p = start(d); acked = []; lock = threading.Lock(); stop = [False]
    def client(cid):
        try:
            s = socket.create_connection(("127.0.0.1", PORT), timeout=5); i = 0
            while not stop[0]:
                k = f"c{cid}:{i}"; s.sendall(enc("SET", k, "v" + k))
                if s.recv(100).startswith(b"+OK"):
                    with lock: acked.append(k)
                i += 1
        except OSError: pass
    ths = [threading.Thread(target=client, args=(c,)) for c in range(24)]
    [t.start() for t in ths]; time.sleep(random.uniform(0.5, 2.0))
    p.send_signal(signal.SIGKILL); p.wait(); stop[0] = True; [t.join() for t in ths]
    p = start(d); s = socket.create_connection(("127.0.0.1", PORT), timeout=10); lost = 0
    for k in acked:
        s.sendall(enc("GET", k)); r = b""
        while not r.endswith(b"\r\n") or (r.startswith(b"$") and not r.endswith(b"\r\n") ): r += s.recv(100)
        if (b"v" + k.encode()) not in r: lost += 1
    print(f"trial {trial} acked={len(acked)} lost={lost}", flush=True); bad += lost
    p.kill(); p.wait()
print("TOTAL LOST", bad)
