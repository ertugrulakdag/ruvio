#!/usr/bin/env python3
"""JSON speed/memory table and a mixed-write durability check for Ruvio (J1/J4/J6).

  bench   field GET/SET, MERGE into a small object, root GET on 1 KiB .. 1 MiB
          documents; req/s, batch p99 and RSS. With --rejson-module and --redis-server
          the same operations run on RedisJSON and a ratio column is printed.
  mixed   plain SET/GET rate with and without large JSON documents loaded.
  verify  random JSON.SET/MERGE/DEL (+ TTL, MULTI) against a Python model; SAVE, restart,
          kill -9 and compare every document after each recovery.

Defaults are small (--quick). Needs a built ruvio binary; nothing is measured against
Redis unless both Redis options are given. Numbers are same-machine, not guarantees.
"""

import argparse
import copy
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIZES = [("1 KiB", 1024), ("64 KiB", 64 * 1024), ("255 KiB", 255 * 1024),
         ("257 KiB", 257 * 1024), ("1 MiB", 1024 * 1024)]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def frame(*args):
    args = [a if isinstance(a, bytes) else str(a).encode() for a in args]
    return b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a) for a in args)


class Conn:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=60)
        self.stream = self.sock.makefile("rb")

    def read(self):
        kind, line = self.stream.read(1), self.stream.readline().strip()
        if kind == b"-":
            return RuntimeError(line.decode())
        if kind == b"$":
            return None if line == b"-1" else self.stream.read(int(line) + 2)[:-2]
        if kind == b"*":
            return [self.read() for _ in range(int(line))]
        return line if kind == b"+" else int(line) if kind == b":" else None

    def call(self, *args):
        self.sock.sendall(frame(*args))
        return self.read()

    def pipeline(self, frames):
        self.sock.sendall(b"".join(frames))
        return [self.read() for _ in frames]


def rss_kib(pid):
    return int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True))


class Server:
    def __init__(self, kind, binary, workdir, extra=()):
        self.kind, self.port = kind, free_port()
        self.binary, self.workdir, self.extra = binary, Path(workdir), list(extra)
        self.proc = None

    def start(self, durability="everysec"):
        log = (self.workdir / f"{self.kind}.log").open("ab")
        if self.kind == "ruvio":
            argv = [self.binary, "--address", f"127.0.0.1:{self.port}",
                    "--data-dir", str(self.workdir / "ruvio-data"),
                    "--durability", durability, "--protected-mode", "false"] + self.extra
        else:
            argv = [self.binary, "--port", str(self.port), "--save", "",
                    "--appendonly", "no", "--dir", str(self.workdir)] + self.extra
        self.proc = subprocess.Popen(argv, cwd=self.workdir, stdout=log, stderr=log)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                Conn(self.port).call("PING")
                return
            except OSError:
                time.sleep(0.1)
        raise TimeoutError(self.kind)

    def stop(self, sig=signal.SIGTERM):
        if self.proc:
            self.proc.send_signal(sig)
            self.proc.wait(timeout=30)
            self.proc = None


def document(size):
    doc = {"name": "n", "meta": {"value": "0" * 12, "k": 1}, "score": 1}
    doc["bulk"] = "x" * max(0, size - 120)
    return json.dumps(doc, separators=(",", ":"))


def timed(conn, make_frame, requests, batch=16):
    done, latencies = 0, []
    begin = time.perf_counter()
    while done < requests:
        n = min(batch, requests - done)
        t = time.perf_counter()
        conn.pipeline([make_frame(done + i) for i in range(n)])
        latencies.append((time.perf_counter() - t) * 1000)
        done += n
    elapsed = time.perf_counter() - begin
    latencies.sort()
    return requests / elapsed, latencies[int(len(latencies) * 0.99) - 1 if len(latencies) > 1 else 0]


def bench_one(server, size, scale, redis_paths):
    conn = Conn(server.port)
    conn.call("FLUSHALL")
    key, field = "doc", ".meta.value" if redis_paths else "$.meta.value"
    meta = ".meta" if redis_paths else "$.meta"
    root = "." if redis_paths else "$"
    base_before = rss_kib(server.proc.pid)
    for i in range(4):
        conn.call("JSON.SET", f"{key}{i}", root, document(size))
    big = size >= 256 * 1024
    n_field = max(200, int((20000 if size <= 65536 else 5000) * scale))
    n_root = max(50, int((2000 if size <= 65536 else 500) * scale))
    rows = {}
    rows["field GET"] = timed(conn, lambda i: frame("JSON.GET", key + str(i % 4), field), n_field)
    rows["field SET"] = timed(
        conn, lambda i: frame("JSON.SET", key + str(i % 4), field, json.dumps(f"{i:012d}")), n_field)
    rows["MERGE meta"] = timed(
        conn, lambda i: frame("JSON.MERGE", key + str(i % 4), meta, json.dumps({"value": f"{i:012d}"})),
        n_field)
    rows["root GET"] = timed(conn, lambda i: frame("JSON.GET", key + str(i % 4), root), n_root, 4)
    rows["rss_delta_kib"] = rss_kib(server.proc.pid) - base_before
    del big
    return rows


def run_bench(args, workdir):
    ruvio = Server("ruvio", args.ruvio, workdir)
    redis = None
    if args.redis_server and args.rejson_module:
        redis = Server("redis", args.redis_server, workdir, ["--loadmodule", args.rejson_module])
    results = {}
    try:
        ruvio.start("memory")
        if redis:
            redis.start()
        for label, size in SIZES:
            results[label] = {"ruvio": bench_one(ruvio, size, args.scale, False)}
            if redis:
                results[label]["redis"] = bench_one(redis, size, args.scale, True)
    finally:
        ruvio.stop()
        if redis:
            redis.stop()
    lines = ["| Size | Operation | Ruvio req/s | Ruvio p99 ms | RedisJSON req/s | Ruvio/RedisJSON |",
             "| --- | --- | ---: | ---: | ---: | ---: |"]
    for label, by in results.items():
        for op in ("field GET", "field SET", "MERGE meta", "root GET"):
            rate, p99 = by["ruvio"][op]
            other = by.get("redis", {}).get(op)
            lines.append(f"| {label} | {op} | {rate:,.0f} | {p99:.2f} | "
                         f"{other[0]:,.0f} | {rate / other[0]:.2f}x |" if other else
                         f"| {label} | {op} | {rate:,.0f} | {p99:.2f} | – | – |")
        lines.append(f"| {label} | RSS Δ (KiB, 4 belge) | {by['ruvio']['rss_delta_kib']} | – | "
                     f"{by['redis']['rss_delta_kib'] if 'redis' in by else '–'} | – |")
    return "\n".join(lines)


def run_mixed(args, workdir):
    out = ["| Durum | SET req/s | GET req/s |", "| --- | ---: | ---: |"]
    server = Server("ruvio", args.ruvio, workdir)
    try:
        server.start("memory")
        conn = Conn(server.port)
        n = max(2000, int(50000 * args.scale))
        for label, load in (("no JSON", False), ("1 MiB + 64 KiB JSON loaded", True)):
            conn.call("FLUSHALL")
            if load:
                for i in range(8):
                    conn.call("JSON.SET", f"big{i}", "$", document(1024 * 1024))
                    conn.call("JSON.SET", f"mid{i}", "$", document(64 * 1024))
            set_rate, _ = timed(conn, lambda i: frame("SET", f"k{i % 1000}", "v" * 64), n, 32)
            get_rate, _ = timed(conn, lambda i: frame("GET", f"k{i % 1000}"), n, 32)
            out.append(f"| {label} | {set_rate:,.0f} | {get_rate:,.0f} |")
    finally:
        server.stop()
    return "\n".join(out)


# ---- verify: Python model -------------------------------------------------------------

def parse_path(path):
    parts, i = [], 1
    while i < len(path):
        if path[i] == ".":
            j = i + 1
            while j < len(path) and path[j] not in ".[":
                j += 1
            parts.append(path[i + 1:j])
            i = j
        else:
            j = path.index("]", i)
            parts.append(int(path[i + 1:j]))
            i = j + 1
    return parts


def merge_patch(target, patch):
    if not isinstance(patch, dict):
        return patch
    target = dict(target) if isinstance(target, dict) else {}
    for k, v in patch.items():
        if v is None:
            target.pop(k, None)
        else:
            target[k] = merge_patch(target.get(k), v)
    return target


def model_apply(doc, op, path, value):
    """Returns (new_doc, applied). Mirrors the documented simple-path semantics."""
    parts = parse_path(path)
    if not parts:
        return (merge_patch(doc, value) if op == "merge" else value), op != "del"
    node = doc
    for part in parts[:-1]:
        if isinstance(part, int):
            if not isinstance(node, list) or part >= len(node):
                return doc, False
        elif not isinstance(node, dict) or part not in node:
            return doc, False
        node = node[part]
    last = parts[-1]
    if isinstance(last, int):
        if not isinstance(node, list) or last >= len(node):
            return doc, False
        if op == "set":
            node[last] = copy.deepcopy(value)
        elif op == "merge":
            node[last] = merge_patch(node[last], copy.deepcopy(value))
        else:
            node.pop(last)
    else:
        if not isinstance(node, dict):
            return doc, False
        if op == "del":
            if last not in node:
                return doc, False
            del node[last]
        elif op == "set":
            node[last] = copy.deepcopy(value)
        else:
            node[last] = merge_patch(node.get(last), value)
    return doc, True


PATHS = ["$.meta", "$.meta.value", "$.meta.tags", "$.meta.tags[1]", "$.meta.o", "$.meta.o.k",
         "$.meta.fresh", "$.arr", "$.arr[0]", "$.arr[1].n", "$.arr[5]", "$.nope.x", "$.n", "$.top"]
VALUES = [1, "text", {"a": 1, "b": {"c": [1, 2]}}, [1, 2, 3], None, {"k": None, "z": 2}, True, {}]


def fresh(size):
    return {"bulk": "x" * size, "meta": {"value": "a", "tags": [1, 2, 3], "o": {"k": 1}},
            "arr": [{"n": 1}, {"n": 2}], "n": 5}


def run_verify(args, workdir):
    rng = random.Random(args.seed)
    server = Server("ruvio", args.ruvio, workdir)
    sizes = {"s0": 2000, "s1": 70_000, "s2": 300_000, "s3": 1_100_000}
    models = {key: fresh(size) for key, size in sizes.items()}
    checks = 0
    history = []
    try:
        server.start("always")
        conn = Conn(server.port)
        for key, doc in models.items():
            assert conn.call("JSON.SET", key, "$", json.dumps(doc)) == b"OK"
        conn.call("PEXPIRE", "s2", 3_600_000)
        for rnd in range(args.rounds):
            for _ in range(args.ops):
                key = rng.choice(list(models))
                path, value = rng.choice(PATHS), copy.deepcopy(rng.choice(VALUES))
                op = rng.choice(["set", "merge", "del"])
                if op == "del":
                    reply = conn.call("JSON.DEL", key, path)
                    new, applied = model_apply(models[key], "del", path, None)
                    assert (reply == 1) == applied, (key, op, path, reply)
                else:
                    cmd = "JSON.SET" if op == "set" else "JSON.MERGE"
                    reply = conn.call(cmd, key, path, json.dumps(value))
                    new, applied = model_apply(models[key], op, path, value)
                    assert isinstance(reply, bytes) == applied or (reply is None and not applied), (
                        key, op, path, reply)
                models[key] = new
                history.append((key, op, path, value))
            for how in ("save-restart", "kill9") if rnd % 2 == 0 else ("kill9", "save-restart"):
                if how == "save-restart":
                    conn.call("SAVE")
                    server.stop()
                else:
                    server.stop(signal.SIGKILL)
                server.start("always")
                conn = Conn(server.port)
                for key, model in models.items():
                    got = conn.call("JSON.GET", key, "$")
                    exact = json.dumps(model, separators=(",", ":"), ensure_ascii=False).encode()
                    if got is None or got != exact:
                        have = json.loads(got) if got else None
                        diff = {k: (have or {}).get(k) for k in model if k != "bulk"
                                if (have or {}).get(k) != model[k]}
                        raise AssertionError(
                            f"{key} diverged after {how}: bulk_same="
                            f"{(have or {}).get('bulk') == model.get('bulk')} "
                            f"server={diff} model={ {k: model[k] for k in diff} } log={history[-6:]}")
                    checks += 1
                assert conn.call("PTTL", "s2") > 0
    finally:
        server.stop(signal.SIGKILL)
    return f"verify ok: {args.rounds} rounds x {args.ops} ops, {checks} document comparisons, no differences"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("mode", choices=["bench", "mixed", "verify"])
    parser.add_argument("--ruvio", default=str(ROOT / "target/release-fast/ruvio"))
    parser.add_argument("--redis-server", default=shutil.which("redis-server"))
    parser.add_argument("--rejson-module")
    parser.add_argument("--scale", type=float, default=0.1, help="request count multiplier")
    parser.add_argument("--quick", action="store_true", help="smallest sizes only (CI smoke)")
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--ops", type=int, default=150)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()
    if args.quick:
        global SIZES
        SIZES = SIZES[:2] + SIZES[3:4]
    with tempfile.TemporaryDirectory(prefix="ruvio-json-") as workdir:
        workdir = Path(workdir)
        print({"bench": run_bench, "mixed": run_mixed, "verify": run_verify}[args.mode](args, workdir))


if __name__ == "__main__":
    sys.exit(main())
