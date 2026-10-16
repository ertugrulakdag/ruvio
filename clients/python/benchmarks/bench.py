# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Micro-benchmark: encode/parse ns and short live GET/SET ops/s (a few seconds total).

    RUVIO_TEST_ADDR=127.0.0.1:17105 python benchmarks/bench.py [--seconds 1.0] [--redis-py]
    RUVIO_CLUSTER_ADDR=127.0.0.1:17330 (with RUVIO_TEST_ADDR) adds the cluster-vs-standalone section

Live numbers need a running server; ``--redis-py`` also measures redis-py (if installed)
against the same server for comparison. Results are single short runs on one machine.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
import tracemalloc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import ruvio  # noqa: E402
from ruvio import _resp  # noqa: E402
from ruvio.asyncio import Ruvio as AsyncRuvio  # noqa: E402

VALUE = "v" * 64


def ns_per(fn, n: int = 200_000) -> float:
    best = float("inf")
    for _ in range(3):
        t = time.perf_counter()
        for _ in range(n):
            fn()
        best = min(best, (time.perf_counter() - t) / n)
    return best * 1e9


def alloc_per(fn, n: int = 10_000) -> float:
    fn()
    tracemalloc.start()
    before = tracemalloc.take_snapshot()
    keep = [fn() for _ in range(n)]
    after = tracemalloc.take_snapshot()
    tracemalloc.stop()
    total = sum(s.size_diff for s in after.compare_to(before, "filename"))
    del keep
    return total / n


def parse_bench() -> None:
    replies = {
        "+OK": b"+OK\r\n",
        ":int": b":123456\r\n",
        "$bulk64": b"$64\r\n" + b"v" * 64 + b"\r\n",
        "$nil": b"$-1\r\n",
        "*10 bulk": b"*10\r\n" + b"$5\r\nhello\r\n" * 10,
    }
    kinds = ["python"] + (["hiredis"] if _resp.hiredis_available() else [])
    print("parse (ns per reply: copy one reply into the read buffer, then parse it):")
    for kind in kinds:
        row = []
        for name, data in replies.items():
            p = _resp.make_parser(kind)
            size = len(data)

            def one(p=p, data=data, size=size):
                view = p.get_buffer()
                view[:size] = data
                p.end += size
                return p.gets()

            assert one() is not _resp.NEED
            row.append(f"{name}={ns_per(one, 100_000):.0f}")
        print(f"  {kind:8s} " + "  ".join(row))
    for kind in kinds:
        p = _resp.make_parser(kind)
        frame = replies["$bulk64"]

        def parse_one(p=p, frame=frame):
            view = p.get_buffer()
            view[: len(frame)] = frame
            p.end += len(frame)
            return p.gets()

        print(f"  {kind:8s} bytes retained per parsed $bulk64 reply (the returned str itself): "
              f"{alloc_per(parse_one):.0f}  (sys.getsizeof of the value: {sys.getsizeof(parse_one())})")
    enc_get = ns_per(lambda: _resp.encode("GET", ("user:1000",)))
    enc_set = ns_per(lambda: _resp.encode("SET", ("user:1000", VALUE)))
    print(f"encode (ns): GET={enc_get:.0f}  SET(64B)={enc_set:.0f}  "
          f"bytes allocated/encode GET={alloc_per(lambda: _resp.encode('GET', ('user:1000',))):.0f}")


class _Fake(ruvio.Ruvio):
    """Captures the encoded frame without I/O (typed-method encode cost)."""

    __slots__ = ()

    def _x(self, conv, data, decode=None):  # type: ignore[override]
        return data


def typed_encode_bench() -> None:
    f = _Fake.__new__(_Fake)
    print(f"typed method encode (ns): get={ns_per(lambda: f.get('user:1000')):.0f}  "
          f"set={ns_per(lambda: f.set('user:1000', VALUE)):.0f}  "
          f"hset={ns_per(lambda: f.hset('h', 'f', VALUE)):.0f}  "
          f"zadd={ns_per(lambda: f.zadd('z', {'m': 1.5})):.0f}")


def timed(fn, seconds: float) -> float:
    n = 0
    end = time.perf_counter() + seconds
    while time.perf_counter() < end:
        for _ in range(100):
            fn()
        n += 100
    return n / seconds


def sync_bench(host: str, port: int, seconds: float, redis_py: bool) -> None:
    r = ruvio.Ruvio(host, port, db=14)
    r.set("bench:k", VALUE)
    print(f"sync serial   ruvio:    GET {timed(lambda: r.get('bench:k'), seconds):>9,.0f} ops/s   "
          f"SET {timed(lambda: r.set('bench:k', VALUE), seconds):>9,.0f} ops/s")
    pipe_n = 0
    end = time.perf_counter() + seconds
    while time.perf_counter() < end:
        p = r.pipeline()
        for _ in range(100):
            p.get("bench:k")
        p.execute()
        pipe_n += 100
    print(f"sync pipeline ruvio:    GET {pipe_n / seconds:>9,.0f} ops/s (100 per batch)")
    r.close()
    if redis_py:
        try:
            import redis
        except ImportError:
            print("redis-py not installed; skipped")
            return
        rp = redis.Redis(host=host, port=port, db=14, decode_responses=True)
        print(f"sync serial   redis-py: GET {timed(lambda: rp.get('bench:k'), seconds):>9,.0f} ops/s   "
              f"SET {timed(lambda: rp.set('bench:k', VALUE), seconds):>9,.0f} ops/s   "
              f"(parser={type(rp.connection_pool.get_connection()._parser).__name__ if hasattr(rp.connection_pool, 'get_connection') else '?'})")
        rp.close()


async def async_run(get, set_, seconds: float, concurrency: int) -> tuple:
    async def worker(op, stop):
        n = 0
        while time.perf_counter() < stop:
            await op()
            n += 1
        return n

    out = []
    for op in (get, set_):
        stop = time.perf_counter() + seconds
        counts = await asyncio.gather(*[worker(op, stop) for _ in range(concurrency)])
        out.append(sum(counts) / seconds)
    return tuple(out)


async def async_bench(host: str, port: int, seconds: float, redis_py: bool) -> None:
    r = AsyncRuvio(host, port, db=14)
    await r.connect()
    await r.set("bench:k", VALUE)
    for c in (1, 64):
        g, s = await async_run(lambda: r.get("bench:k"), lambda: r.set("bench:k", VALUE), seconds, c)
        print(f"async c={c:<3d}   ruvio:    GET {g:>9,.0f} ops/s   SET {s:>9,.0f} ops/s   (1 connection)")
    await r.aclose()
    if redis_py:
        try:
            import redis.asyncio as aredis
        except ImportError:
            return
        rp = aredis.Redis(host=host, port=port, db=14, decode_responses=True, max_connections=64)
        for c in (1, 64):
            g, s = await async_run(lambda: rp.get("bench:k"), lambda: rp.set("bench:k", VALUE), seconds, c)
            print(f"async c={c:<3d}   redis-py: GET {g:>9,.0f} ops/s   SET {s:>9,.0f} ops/s   (pool, up to 64 connections)")
        await rp.aclose()


def routing_bench() -> None:
    from ruvio import _cluster as cl

    f = _resp.encode("GET", ("user:1000",))
    frame = _resp.encode("SET", ("user:1000", "v" * 1000))
    print(f"cluster routing cost (ns, no I/O): frame_head+key_slot GET={ns_per(lambda: cl.key_slot(cl.frame_head(f)[1])):.0f}  "
          f"SET(1000B value)={ns_per(lambda: cl.key_slot(cl.frame_head(frame)[1])):.0f}  "
          f"key_slot(user:1000)={ns_per(lambda: cl.key_slot(b'user:1000')):.0f}")


def cluster_bench(saddr: str, caddr: str, seconds: float) -> None:
    """Same 64 spread keys on a plain server (1 shard, 1 connection) and on the 3-shard cluster."""
    import itertools

    keys = [f"bench:{i}" for i in range(64)]
    sh, sp = saddr.rsplit(":", 1)
    ch, cp = caddr.rsplit(":", 1)
    plain = ruvio.Ruvio(sh, int(sp), db=14)
    clu = ruvio.Ruvio(ch, int(cp), cluster=True)
    for name, r in (("standalone", plain), ("cluster   ", clu)):
        for k in keys:
            r.set(k, VALUE)
        cyc = itertools.cycle(keys)
        g = timed(lambda: r.get(next(cyc)), seconds)
        s_ = timed(lambda: r.set(next(cyc), VALUE), seconds)
        print(f"sync serial   {name}: GET {g:>9,.0f} ops/s   SET {s_:>9,.0f} ops/s")
    for name, r in (("standalone", plain), ("cluster   ", clu)):
        n = 0
        end = time.perf_counter() + seconds
        while time.perf_counter() < end:
            p = r.pipeline()
            for k in keys:
                p.get(k)
            p.execute()
            n += len(keys)
        print(f"sync pipeline {name}: GET {n / seconds:>9,.0f} ops/s (64 spread keys per batch)")
    plain.close()
    clu.close()

    async def run() -> None:
        for name, mk in (("standalone", lambda: AsyncRuvio(sh, int(sp), db=14)), ("cluster   ", lambda: AsyncRuvio(ch, int(cp), cluster=True))):
            r = mk()
            await r.connect()
            for conc in (1, 64):
                cyc = itertools.cycle(keys)
                g, s_ = await async_run(lambda: r.get(next(cyc)), lambda: r.set(next(cyc), VALUE), seconds, conc)
                print(f"async c={conc:<3d}   {name}: GET {g:>9,.0f} ops/s   SET {s_:>9,.0f} ops/s")
            await r.aclose()

    asyncio.run(run())


def cluster_cost(port: int, seconds: float = 4.0, warmup: float = 1.0) -> None:
    """Plain vs cluster CPU per request, one client, GET/SET at P=1/16/64 (process CPU, not wall time)."""
    import resource

    tag = "{" + os.environ["BENCH_TAG"] + "}" if os.environ.get("BENCH_TAG") else ""  # one hash tag: routing on one node
    keys = [f"{tag}key:{i}" for i in range(4096)]
    val = "x" * 32

    def run(kind: str, op: str, p: int) -> None:
        r = ruvio.Ruvio("127.0.0.1", port, cluster=(kind == "cluster"))
        r.connect()
        pos = 0
        n_keys = len(keys)

        def step() -> int:
            nonlocal pos
            if pos + p > n_keys:
                pos = 0
            sl = keys[pos : pos + p]
            pos += p
            if p == 1:
                r.get(sl[0]) if op == "GET" else r.set(sl[0], val)
            else:
                pl = r.pipeline()
                if op == "GET":
                    for k in sl:
                        pl.get(k)
                else:
                    for k in sl:
                        pl.set(k, val)
                pl.execute()
            return p

        end = time.perf_counter() + warmup
        while time.perf_counter() < end:
            step()
        ru0, t0, ops = resource.getrusage(resource.RUSAGE_SELF), time.perf_counter(), 0
        end = t0 + seconds
        while time.perf_counter() < end:
            for _ in range(20):
                ops += step()
        t1, ru1 = time.perf_counter(), resource.getrusage(resource.RUSAGE_SELF)
        r.close()
        print(f"{kind} {op} P={p} ops/s={ops / (t1 - t0):.0f} user_ns/op={(ru1.ru_utime - ru0.ru_utime) * 1e9 / ops:.0f} "
              f"sys_ns/op={(ru1.ru_stime - ru0.ru_stime) * 1e9 / ops:.0f}", flush=True)

    for op in ("GET", "SET"):
        for p in (1, 16, 64):
            for kind in ("plain", "cluster"):
                run(kind, op, p)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=1.0)
    ap.add_argument("--redis-py", action="store_true")
    ap.add_argument("--cluster-cost", type=int, metavar="PORT", help="plain vs cluster CPU per request against a per-shard server")
    a = ap.parse_args()
    if a.cluster_cost:
        cluster_cost(a.cluster_cost)
        return
    print(f"python {sys.version.split()[0]}, hiredis available: {_resp.hiredis_available()}")
    parse_bench()
    typed_encode_bench()
    routing_bench()
    saddr, caddr = os.environ.get("RUVIO_TEST_ADDR"), os.environ.get("RUVIO_CLUSTER_ADDR")
    if saddr and caddr:
        print("cluster vs standalone (standalone: RUVIO_TEST_ADDR, cluster: RUVIO_CLUSTER_ADDR):")
        cluster_bench(saddr, caddr, a.seconds)
    addr = os.environ.get("RUVIO_TEST_ADDR")
    if not addr:
        print("RUVIO_TEST_ADDR not set: live benchmark skipped")
        return
    host, port = addr.rsplit(":", 1)
    sync_bench(host, int(port), a.seconds, a.redis_py)
    asyncio.run(async_bench(host, int(port), a.seconds, a.redis_py))


if __name__ == "__main__":
    main()
