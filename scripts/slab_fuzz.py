#!/usr/bin/env python3
"""Random list/hash traffic against Ruvio and Redis; every reply and the final state must match.

Targets the small-collection slab records: values around the 77-byte slot limit, growth and
shrink across it, RENAME, COPY, EXPIRE and DEL. With --restart the Ruvio server is killed and
reopened (durability always) and the state compared again.

    python3 scripts/slab_fuzz.py --ruvio target/release-fast/ruvio [--ops 4000] [--restart]
"""
import argparse, random, socket, subprocess, tempfile, time, os, signal
from pathlib import Path


def enc(*a):
    a = [x if isinstance(x, bytes) else str(x).encode() for x in a]
    return b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a)


def read(f):
    line = f.readline()
    t, rest = line[:1], line[1:-2]
    if t in b"+-:":
        return (t, rest)
    if t == b"$":
        n = int(rest)
        return (t, None if n < 0 else f.read(n + 2)[:-2])
    if t == b"*":
        n = int(rest)
        return (t, None if n < 0 else [read(f) for _ in range(n)])
    raise ValueError(line)


def norm(cmd, r):
    # Unordered replies and error text that legitimately differs.
    if r[0] == b"-":
        return (b"-", r[1].split(b" ")[0])
    if cmd[0] == b"SMEMBERS" and r[0] == b"*":
        return (b"*", sorted(x[1] for x in r[1]))
    if cmd[0] in (b"HGETALL", b"HKEYS", b"HVALS") and r[0] == b"*":
        items = [x[1] for x in r[1]]
        if cmd[0] == b"HGETALL":
            pairs = sorted(zip(items[0::2], items[1::2]))
            return (b"*", pairs)
        return (b"*", sorted(items))
    if cmd[0] == b"TTL":
        return (r[0], r[1] != b"-1" if r[0] == b":" and r[1] not in (b"-2",) else r[1])
    return r


def conn(port):
    s = socket.create_connection(("127.0.0.1", port))
    return s, s.makefile("rwb", buffering=0)


def wait(port):
    for _ in range(100):
        try:
            conn(port)[0].close()
            return
        except OSError:
            time.sleep(0.1)
    raise SystemExit("server did not start")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ruvio", default="target/release-fast/ruvio")
    ap.add_argument("--ops", type=int, default=4000)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--restart", action="store_true")
    args = ap.parse_args()
    tmp = Path(tempfile.mkdtemp(prefix="slab-fuzz-"))
    rp, dp = 7521, 7522

    def start_ruvio():
        return subprocess.Popen(
            [args.ruvio, "--address", f"127.0.0.1:{rp}", "--durability", "always", "--shards", "1", "--data-dir", str(tmp / "r")],
            cwd=tmp, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    ruvio = start_ruvio()
    redis = subprocess.Popen(["redis-server", "--port", str(dp), "--save", "", "--appendonly", "no", "--dir", str(tmp)], stdout=subprocess.DEVNULL)
    try:
        wait(rp); wait(dp)
        rs, rf = conn(rp); ds, df = conn(dp)
        rng = random.Random(args.seed)
        keys = [f"l{i}" for i in range(6)] + [f"h{i}" for i in range(6)] + [f"s{i}" for i in range(4)] + [f"z{i}" for i in range(4)]

        def val():
            return "v" * rng.choice([0, 1, 2, 5, 9, 17, 30, 60, 90, 200]) + str(rng.randrange(10))

        def both(*cmd):
            ruvio_reply = (rf.write(enc(*cmd)), read(rf))[1]
            redis_reply = (df.write(enc(*cmd)), read(df))[1]
            c = [x if isinstance(x, bytes) else str(x).encode() for x in cmd]
            if norm(c, ruvio_reply) != norm(c, redis_reply):
                raise SystemExit(f"DIFF after {cmd}: ruvio={ruvio_reply} redis={redis_reply}")

        def check_all():
            for k in keys:
                both("TYPE", k)
                both("TTL", k)
                if k.startswith("l"):
                    both("LRANGE", k, 0, -1)
                    both("LLEN", k)
                elif k.startswith("h"):
                    both("HGETALL", k)
                    both("HLEN", k)
                elif k.startswith("s"):
                    both("SMEMBERS", k)
                    both("SCARD", k)
                else:
                    both("ZRANGE", k, 0, -1, "WITHSCORES")
                    both("ZCARD", k)

        for step in range(args.ops):
            k = rng.choice(keys)
            lst, hsh = rng.choice(keys[:6]), rng.choice(keys[6:12])
            sst, zst = rng.choice(keys[12:16]), rng.choice(keys[16:])
            op = rng.randrange(24)
            if op == 0: both("RPUSH", lst, val())
            elif op == 1: both("LPUSH", lst, val(), val())
            elif op == 2: both("LPOP", lst)
            elif op == 3: both("RPOP", lst)
            elif op == 4: both("LSET", lst, rng.randrange(-2, 4), val())
            elif op == 5: both("LRANGE", lst, 0, -1)
            elif op == 6: both("LINDEX", lst, rng.randrange(-2, 4))
            elif op == 7: both("HSET", hsh, f"f{rng.randrange(8)}", val())
            elif op == 8: both("HDEL", hsh, f"f{rng.randrange(8)}")
            elif op == 9: both("HGET", hsh, f"f{rng.randrange(8)}")
            elif op == 10: both("HGETALL", hsh)
            elif op == 11: both("EXPIRE", k, rng.choice([1000, 5000]))
            elif op == 12: both("COPY", k, rng.choice(keys), "REPLACE")
            elif op == 13: both("RENAME", k, rng.choice(keys))
            elif op == 14: both("DEL", k)
            elif op == 15: both("LTRIM", lst, 0, rng.randrange(0, 4))
            elif op == 16: both("SADD", sst, *[f"m{rng.randrange(40)}" + "x" * rng.choice([0, 0, 5, 30]) for _ in range(rng.randrange(1, 4))])
            elif op == 17: both("SREM", sst, f"m{rng.randrange(40)}")
            elif op == 18: both("SISMEMBER", sst, f"m{rng.randrange(40)}")
            elif op == 19: both("ZADD", zst, rng.randrange(-5, 50), f"z{rng.randrange(30)}" + "y" * rng.choice([0, 0, 6, 40]))
            elif op == 20: both("ZREM", zst, f"z{rng.randrange(30)}")
            elif op == 21: both("ZINCRBY", zst, rng.randrange(1, 5), f"z{rng.randrange(30)}")
            elif op == 22: both("ZSCORE", zst, f"z{rng.randrange(30)}")
            else: both("SCARD", sst)
            if step % 200 == 0:
                check_all()
        check_all()
        print(f"{args.ops} ops, state equal")

        if args.restart:
            rs.close()
            ruvio.send_signal(signal.SIGKILL); ruvio.wait()
            ruvio = start_ruvio(); wait(rp)
            rs, rf = conn(rp)
            check_all()
            print("state equal after kill -9 and restart")
    finally:
        for p in (ruvio, redis):
            p.terminate()
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()


if __name__ == "__main__":
    main()
