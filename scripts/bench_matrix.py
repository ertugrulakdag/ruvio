#!/usr/bin/env python3
"""Measurement matrix: Ruvio vs Redis (io-threads off/4) vs Dragonfly (if present).

Scenarios: uniform, zipf, p1-50, p1-1000, churn-lru, snapshot-p99, zset-rank, scan-full.
Output: <out>/<date>/matrix.csv and summary.md (one table, <= 40 rows).

  python3 scripts/bench_matrix.py --quick             # small scale, about 5 min per run
  python3 scripts/bench_matrix.py --quick --runs 1    # one run, for a fast dev loop
  python3 scripts/bench_matrix.py                     # full: 10M keys, 15 min churn (hours)

Each scenario runs `--runs` times (default 3), each on a fresh server, and every number
reported is the median of those runs. Memory is reported as total RSS and split into
heap (anonymous pages: the data) and file-backed pages (mostly the binary's code), so a
bigger binary is not mistaken for a data-structure regression. The split needs Linux
/proc; elsewhere only the total is reported (from `ps`) and the split reads nan.

Load generator is a built-in Python client (multiprocessing + selectors); memtier_benchmark
is used by no scenario. Client and server share the same machine, so absolute numbers are
client-bound; compare targets with each other, not with published figures.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import math
import multiprocessing as mp
import os
import random
import selectors
import shutil
import socket
import statistics
import subprocess
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VALUE = b"x" * 64


def frame(*parts: bytes) -> bytes:
    return b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n%s\r\n" % (len(p), p) for p in parts)


def count_replies(buf: bytes, want: int | None = None):
    """Return (complete replies, consumed bytes, nil bulk count). Handles + - : $ replies."""
    pos, n, nil, size = 0, 0, 0, len(buf)
    while pos < size:
        end = buf.find(b"\r\n", pos)
        if end < 0:
            break
        kind = buf[pos]
        if kind == 36:  # '$'
            ln = int(buf[pos + 1:end])
            if ln < 0:
                nil += 1
                nxt = end + 2
            else:
                nxt = end + 2 + ln + 2
                if nxt > size:
                    break
        else:
            nxt = end + 2
        pos = nxt
        n += 1
    return n, pos, nil


def quote_ranks(rng: random.Random, n: int, s: float):
    """Zipf-like rank in [0, n) via inverse CDF of the continuous approximation (s < 1)."""
    e = 1.0 - s
    top = n ** e - 1.0
    return lambda: min(n - 1, int((rng.random() * top + 1.0) ** (1.0 / e)) - 1)


def make_pool(spec: dict, seed: int, size: int = 32768):
    rng = random.Random(seed)
    loaded, kind = spec["loaded"], spec["kind"]
    if kind == "zset":
        pool = []
        for _ in range(size):
            m = b"m%d" % rng.randrange(loaded)
            if rng.random() < 0.8:
                pool.append(frame(b"ZRANK", b"z", m))
            else:
                pool.append(frame(b"ZADD", b"z", b"%d" % rng.randrange(10**6), m))
        return pool
    space = int(loaded * 1.111)  # ~10% of accesses fall past the loaded range -> miss
    zipf = quote_ranks(rng, space, spec["zipf"]) if spec.get("zipf") else None
    pool = []
    for _ in range(size):
        i = zipf() if zipf else rng.randrange(space)
        if zipf:
            i = (i * 2654435761) % space
        key = b"key:%d" % i
        if rng.random() < spec["get_ratio"]:
            pool.append(frame(b"GET", key))
        elif rng.random() < spec.get("ttl", 0.0):
            pool.append(frame(b"SET", key, VALUE, b"EX", b"120"))
        else:
            pool.append(frame(b"SET", key, VALUE))
    return pool


def worker(port, nconn, pipeline, duration, spec, seed, out):
    pool = make_pool(spec, seed)
    rng = random.Random(seed + 1)
    sel = selectors.DefaultSelector()
    conns = []
    for _ in range(nconn):
        s = socket.create_connection(("127.0.0.1", port))
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.setblocking(False)
        st = {"sock": s, "buf": b"", "pending": 0, "t0": 0.0}
        conns.append(st)
        sel.register(s, selectors.EVENT_READ, st)
    lat, ops, nils = [], 0, 0
    stride = 1 if pipeline == 1 else max(1, pipeline)

    def send(st):
        i = rng.randrange(len(pool) - pipeline + 1) if pipeline > 1 else rng.randrange(len(pool))
        st["pending"] = pipeline
        st["t0"] = time.perf_counter()
        st["sock"].sendall(b"".join(pool[i:i + pipeline]))

    t_end = time.monotonic() + duration
    for st in conns:
        send(st)
    while time.monotonic() < t_end:
        for key, _ in sel.select(timeout=0.2):
            st = key.data
            try:
                data = st["sock"].recv(262144)
            except BlockingIOError:
                continue
            if not data:
                continue
            st["buf"] += data
            n, used, nil = count_replies(st["buf"])
            if n and used:
                st["buf"] = st["buf"][used:]
            nils += nil
            st["pending"] -= n
            if st["pending"] <= 0:
                now = time.perf_counter()
                lat.append(now - st["t0"])
                ops += pipeline
                send(st)
    del stride
    out.put({"ops": ops, "nil": nils, "lat": lat[:: max(1, len(lat) // 20000)]})


def pctl(values, q):
    if not values:
        return float("nan")
    values = sorted(values)
    return values[min(len(values) - 1, int(q * len(values)))]


def run_load(port, conns, pipeline, duration, spec, procs, extra=None):
    """Run load in `procs` processes. `extra` is an optional thread started alongside."""
    q = mp.Queue()
    per = max(1, conns // procs)
    ps = [mp.Process(target=worker, args=(port, per, pipeline, duration, spec, 100 + k, q))
          for k in range(procs)]
    t0 = time.monotonic()
    for p in ps:
        p.start()
    if extra:
        extra()
    results = []
    for _ in ps:
        try:
            results.append(q.get(timeout=duration + 60))
        except Exception:
            raise RuntimeError("load worker died (connection refused/reset?)")
    for p in ps:
        p.join()
    wall = time.monotonic() - t0
    lat = [x for r in results for x in r["lat"]]
    ops = sum(r["ops"] for r in results)
    return {"ops_s": ops / duration, "nil": sum(r["nil"] for r in results), "ops": ops,
            "p50": pctl(lat, 0.50), "p99": pctl(lat, 0.99), "p999": pctl(lat, 0.999), "wall": wall}


# ---------------------------------------------------------------- servers

class Server:
    def __init__(self, name, argv, port, cwd, env=None):
        self.name, self.port = name, port
        self.proc = subprocess.Popen(argv, cwd=cwd, env=env, stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                if self.cmd(b"PING").startswith(b"+PONG"):
                    return
            except OSError:
                time.sleep(0.05)
        raise TimeoutError(name)

    def cmd(self, *parts: bytes, timeout=60.0) -> bytes:
        with socket.create_connection(("127.0.0.1", self.port), timeout=timeout) as s:
            s.sendall(frame(*parts))
            return s.recv(1 << 20)

    def rss_parts(self) -> tuple[float, float, float]:
        """(total, heap, file-backed) resident MiB. Heap is RssAnon; file-backed is
        RssFile + RssShmem, mostly the mapped binary."""
        fields = {}
        try:
            for line in Path(f"/proc/{self.proc.pid}/status").read_text().splitlines():
                name, _, value = line.partition(":")
                if name in ("VmRSS", "RssAnon", "RssFile", "RssShmem"):
                    fields[name] = int(value.split()[0]) / 1024
        except OSError:
            out = subprocess.run(["ps", "-o", "rss=", "-p", str(self.proc.pid)],
                                 capture_output=True, text=True, check=False).stdout.strip()
            total = int(out) / 1024 if out else float("nan")
            return total, float("nan"), float("nan")
        nan = float("nan")
        file_mb = fields.get("RssFile", nan) + fields.get("RssShmem", 0.0)
        return fields.get("VmRSS", nan), fields.get("RssAnon", nan), file_mb

    def rss_mb(self) -> float:
        return self.rss_parts()[0]

    def stop(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.kill()


TARGETS = ["ruvio", "redis", "redis-io4", "dragonfly"]


def start(args, target, port, work: Path, maxmemory=0, durable=False):
    d = Path(tempfile.mkdtemp(prefix=f"{target}-", dir=work))
    if target == "ruvio":
        argv = [str(args.ruvio), "--address", f"127.0.0.1:{port}", "--shards", "1",
                "--durability", "everysec" if durable else "memory", "--data-dir", str(d / "data"),
                "--max-connections", "20000", "--max-connections-per-ip", "20000"]
        env = None
        if maxmemory:
            # --maxmemory only sets the process limit; eviction needs the per-shard budget.
            argv += ["--maxmemory", str(maxmemory), "--maxmemory-policy", "allkeys-lru"]
            env = dict(os.environ, RUVIO_MAX_SHARD_BYTES=str(maxmemory),
                       RUVIO_MEMORY_HEADROOM_BYTES="0")
        return Server(target, argv, port, d, env)
    if target in ("redis", "redis-io4"):
        argv = [args.redis_server, "--bind", "127.0.0.1", "--port", str(port), "--dir", str(d),
                "--save", "", "--appendonly", "yes" if durable else "no", "--appendfsync", "everysec"]
        if target == "redis-io4":
            argv += ["--io-threads", "4", "--io-threads-do-reads", "yes"]
        if maxmemory:
            argv += ["--maxmemory", str(maxmemory), "--maxmemory-policy", "allkeys-lru"]
        return Server(target, argv, port, d)
    raise KeyError(target)


def load_strings(srv, n, batch=2000):
    with socket.create_connection(("127.0.0.1", srv.port)) as s:
        for lo in range(0, n, batch):
            hi = min(n, lo + batch)
            s.sendall(b"".join(frame(b"SET", b"key:%d" % i, VALUE) for i in range(lo, hi)))
            need, buf = hi - lo, b""
            while need > 0:
                buf += s.recv(1 << 20)
                got, used, _ = count_replies(buf)
                need -= got
                buf = buf[used:]


def load_zset(srv, n, batch=2000):
    with socket.create_connection(("127.0.0.1", srv.port)) as s:
        for lo in range(0, n, batch):
            hi = min(n, lo + batch)
            s.sendall(b"".join(frame(b"ZADD", b"z", b"%d" % (i * 7 % 10**6), b"m%d" % i) for i in range(lo, hi)))
            need, buf = hi - lo, b""
            while need > 0:
                buf += s.recv(1 << 20)
                got, used, _ = count_replies(buf)
                need -= got
                buf = buf[used:]


# ---------------------------------------------------------------- scenarios

def ms(x):
    return round(x * 1000, 3)


def rss_metrics(srv):
    total, anon, file_mb = srv.rss_parts()
    return [("rss_mb", round(total, 1)), ("rss_heap_mb", round(anon, 1)),
            ("rss_file_mb", round(file_mb, 1))]


def sc_mixed(args, srv, zipf):
    n = args.keys
    load_strings(srv, n)
    spec = {"kind": "kv", "loaded": n, "get_ratio": 0.9, "zipf": 0.99 if zipf else None}
    r = run_load(srv.port, 50, 16, args.duration, spec, procs=2)
    miss = r["nil"] / max(1, r["ops"] * 0.9)
    return [("ops_s", round(r["ops_s"])), ("batch_p99_ms", ms(r["p99"])),
            ("miss_pct", round(miss * 100, 1))] + rss_metrics(srv)


def sc_p1(args, srv, conns):
    n = min(args.keys, 1_000_000)
    load_strings(srv, n)
    spec = {"kind": "kv", "loaded": n, "get_ratio": 0.9}
    r = run_load(srv.port, conns, 1, args.duration, spec, procs=2 if conns >= 100 else 1)
    return [("ops_s", round(r["ops_s"])), ("p50_us", round(r["p50"] * 1e6)),
            ("p99_us", round(r["p99"] * 1e6)), ("p99.9_us", round(r["p999"] * 1e6))]


def sc_churn(args, srv):
    spec = {"kind": "kv", "loaded": args.churn_keys, "get_ratio": 0.5, "ttl": 0.2}
    samples, stop = [], threading.Event()

    def sample():
        while not stop.is_set():
            samples.append(srv.rss_parts())
            stop.wait(args.rss_interval)

    th = threading.Thread(target=sample)
    th.start()
    r = run_load(srv.port, 50, 16, args.churn_duration, spec, procs=2)
    stop.set()
    th.join()
    end = samples[-1]
    return [("ops_s", round(r["ops_s"])), ("rss_peak_mb", round(max(x[0] for x in samples), 1)),
            ("rss_end_mb", round(end[0], 1)), ("rss_heap_end_mb", round(end[1], 1)),
            ("rss_file_end_mb", round(end[2], 1)), ("rss_samples", len(samples))]


def sc_snapshot(args, srv):
    n = args.keys
    load_strings(srv, n)
    spec = {"kind": "kv", "loaded": n, "get_ratio": 0.5}
    peak, stop = [srv.rss_parts()], threading.Event()

    def trigger():
        def go():
            time.sleep(args.duration * 0.3)
            srv.cmd(b"BGSAVE")
            while not stop.is_set():
                peak.append(srv.rss_parts())
                stop.wait(0.2)
        threading.Thread(target=go, daemon=True).start()

    r = run_load(srv.port, 50, 1, args.duration, spec, procs=1, extra=trigger)
    stop.set()
    return [("ops_s", round(r["ops_s"])), ("p99_us", round(r["p99"] * 1e6)),
            ("p99.9_us", round(r["p999"] * 1e6)), ("rss_peak_mb", round(max(x[0] for x in peak), 1)),
            ("rss_heap_peak_mb", round(max(x[1] for x in peak), 1))]


def sc_zset(args, srv):
    n = args.zset_members
    load_zset(srv, n)
    r = run_load(srv.port, 50, 16, args.duration, {"kind": "zset", "loaded": n}, procs=2)
    return [("ops_s", round(r["ops_s"])), ("batch_p99_ms", ms(r["p99"]))] + rss_metrics(srv)


def sc_scan(args, srv):
    n = args.keys
    load_strings(srv, n)
    spec = {"kind": "kv", "loaded": n, "get_ratio": 1.0}
    seen, took = [0], [0.0]

    def scan():
        def go():
            time.sleep(0.5)
            t0, cur, total = time.monotonic(), b"0", 0
            with socket.create_connection(("127.0.0.1", srv.port), timeout=120) as s:
                while True:
                    s.sendall(frame(b"SCAN", cur, b"COUNT", b"1000"))
                    buf = b""
                    while True:
                        buf += s.recv(1 << 20)
                        if buf.count(b"\r\n") >= 4 and buf.endswith(b"\r\n"):
                            parts = buf.split(b"\r\n")
                            want = int(parts[0][1:]) if parts[0][:1] == b"*" else 0
                            cnt = int(parts[3][1:]) if parts[3][:1] == b"*" else -1
                            if cnt >= 0 and buf.count(b"\r\n") >= 4 + 2 * cnt:
                                break
                    parts = buf.split(b"\r\n")
                    cur = parts[2]
                    total += int(parts[3][1:])
                    if cur == b"0":
                        break
            seen[0], took[0] = total, time.monotonic() - t0
        t = threading.Thread(target=go)
        t.start()
        scan.t = t

    r = run_load(srv.port, 20, 1, args.duration, spec, procs=1, extra=scan)
    scan.t.join()
    return [("scan_s", round(took[0], 2)), ("scan_keys", seen[0]),
            ("others_p99_us", round(r["p99"] * 1e6)), ("others_ops_s", round(r["ops_s"]))]


SCENARIOS = ["uniform", "zipf", "p1-50", "p1-1000", "churn-lru", "snapshot-p99", "zset-rank", "scan-full"]


def available_targets(args):
    out = ["ruvio"]
    if shutil.which(args.redis_server):
        out += ["redis", "redis-io4"]
    if shutil.which("dragonfly"):
        out.append("dragonfly")  # not wired: skipped below, noted in summary
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true", help="small scale, 5-8 s per run, < 5 min total")
    ap.add_argument("--ruvio", type=Path, default=ROOT / "target/release/ruvio")
    ap.add_argument("--redis-server", default="redis-server")
    ap.add_argument("--scenarios", default=",".join(SCENARIOS))
    ap.add_argument("--out", type=Path, default=ROOT / "benchmark/results")
    ap.add_argument("--base-port", type=int, default=6500)
    ap.add_argument("--runs", type=int, default=3,
                    help="fresh-server runs per scenario and target; numbers are their median")
    a = ap.parse_args()
    if a.quick:
        a.keys, a.duration, a.churn_duration, a.rss_interval = 100_000, 5, 8, 1
        a.zset_members, a.churn_keys, a.churn_mem = 100_000, 400_000, 24 << 20
    else:
        a.keys, a.duration, a.churn_duration, a.rss_interval = 10_000_000, 60, 900, 5
        a.zset_members, a.churn_keys, a.churn_mem = 1_000_000, 20_000_000, 1 << 30

    if not a.ruvio.exists():
        raise SystemExit(f"{a.ruvio} missing; cargo build --release --bin ruvio")
    day = a.out / dt.date.today().isoformat()
    day.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="bench-matrix-"))
    targets = [t for t in available_targets(a) if t != "dragonfly"]
    skipped = [t for t in TARGETS if t not in targets]
    rows, port = [], a.base_port
    for sc in a.scenarios.split(","):
        for t in targets:
            runs, t0 = [], time.monotonic()
            for _ in range(max(1, a.runs)):
                port += 1
                runs.append(run_once(a, sc, t, port, work))
            m = median_metrics(runs)
            for k, v in m:
                rows.append((sc, t, k, v))
            print(f"{sc:13s} {t:10s} {time.monotonic() - t0:5.1f}s {m}", flush=True)

    with open(day / "matrix.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["scenario", "target", "metric", "value"])
        w.writerows(rows)
    shutil.rmtree(work, ignore_errors=True)
    write_summary(day / "summary.md", rows, targets, skipped, a)
    print(f"wrote {day}/matrix.csv and summary.md")


def median_metrics(runs):
    """Per metric, the median over the runs that produced it. A run that failed is left
    out; if every run failed, the first failure is reported."""
    good = [m for m in runs if not any(k == "error" for k, _ in m)]
    if not good:
        return runs[0]
    out = []
    for k, _ in good[0]:
        values = [v for m in good for key, v in m if key == k]
        numeric = [v for v in values if isinstance(v, (int, float)) and not math.isnan(v)]
        if not numeric:
            out.append((k, values[0]))
            continue
        med = statistics.median(numeric)
        out.append((k, round(med, 3) if isinstance(med, float) else med))
    return out


def run_once(a, sc, t, port, work):
    """One fresh server running scenario `sc`; its metrics, or the error it hit."""
    mem = a.churn_mem if sc == "churn-lru" else 0
    srv = start(a, t, port, work, maxmemory=mem, durable=(sc == "snapshot-p99"))
    try:
        if sc == "uniform":
            m = sc_mixed(a, srv, False)
        elif sc == "zipf":
            m = sc_mixed(a, srv, True)
        elif sc == "p1-50":
            m = sc_p1(a, srv, 50)
        elif sc == "p1-1000":
            m = sc_p1(a, srv, 1000)
        elif sc == "churn-lru":
            m = sc_churn(a, srv)
        elif sc == "snapshot-p99":
            m = sc_snapshot(a, srv)
        elif sc == "zset-rank":
            m = sc_zset(a, srv)
        elif sc == "scan-full":
            m = sc_scan(a, srv)
        else:
            raise SystemExit(f"unknown scenario {sc}")
    except Exception as e:  # keep the matrix going; record the failure
        m = [("error", type(e).__name__ + ":" + str(e)[:60])]
    finally:
        srv.stop()
    return m


def write_summary(path, rows, targets, skipped, a):
    keys = []
    for sc, _, k, _ in rows:
        if (sc, k) not in keys:
            keys.append((sc, k))
    cell = {(sc, t, k): v for sc, t, k, v in rows}
    mode = "quick (small scale)" if a.quick else "full"
    lines = [f"# Matrix summary ({mode}, {dt.date.today()})", "",
             f"keys={a.keys:,} duration={a.duration}s churn={a.churn_duration}s zset={a.zset_members:,} "
             f"| median of {max(1, a.runs)} runs | skipped: {', '.join(skipped) or 'none'}", "",
             ("RSS: rss = total, heap = anonymous pages (data), file = file-backed pages "
              "(mostly the binary)."), "",
             "| scenario | metric | " + " | ".join(targets) + " |",
             "|---|---|" + "---|" * len(targets)]
    for sc, k in keys[:40]:
        lines.append(f"| {sc} | {k} | " + " | ".join(str(cell.get((sc, t, k), "-")) for t in targets) + " |")
    path.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
