#!/usr/bin/env python3
"""Check: CHANGES SUBSCRIBE resumes where it left off after a restart, with no loss or repeats;
with a small cdc_retain_bytes the WAL stays bounded and a lagging cursor gets a resync.

Usage: cdc_resume.py <ruvio-binary>
"""
import os, shutil, signal, socket, subprocess, sys, tempfile, time

BINARY = sys.argv[1]
PORT = 7931


def enc(*parts):
    out = b"*%d\r\n" % len(parts)
    for p in parts:
        p = p if isinstance(p, bytes) else str(p).encode()
        out += b"$%d\r\n%s\r\n" % (len(p), p)
    return out


class Conn:
    def __init__(self):
        self.s = socket.create_connection(("127.0.0.1", PORT))
        self.s.settimeout(5)
        self.buf = b""

    def fill(self):
        chunk = self.s.recv(65536)
        if not chunk:
            raise EOFError
        self.buf += chunk

    def line(self):
        while b"\r\n" not in self.buf:
            self.fill()
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def value(self):
        line = self.line()
        t, rest = line[:1], line[1:]
        if t in (b"+", b"-", b":"):
            return rest.decode()
        if t == b"_":
            return None
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                self.fill()
            data, self.buf = self.buf[:n], self.buf[n + 2:]
            return data.decode()
        if t in (b"*", b">"):
            n = int(rest)
            return None if n < 0 else [self.value() for _ in range(n)]
        raise ValueError(line)

    def call(self, *parts):
        self.s.sendall(enc(*parts))
        return self.value()


def start(data, retain, segment):
    env = dict(os.environ, RUVIO_ADDR=f"127.0.0.1:{PORT}", RUVIO_DATA_DIR=data,
               RUVIO_DURABILITY="always", RUVIO_CDC_RETAIN_BYTES=str(retain),
               RUVIO_WAL_SEGMENT_BYTES=str(segment), RUVIO_SNAPSHOT_INTERVAL_MS="0")
    proc = subprocess.Popen([BINARY], cwd=data, env=env, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            Conn().call("PING")
            return proc
        except OSError:
            time.sleep(0.1)
    raise SystemExit("server did not start")


def stop(proc):
    proc.send_signal(signal.SIGKILL)
    proc.wait()


def events(conn, want, resp3=False):
    seen = []
    while len(seen) < want:
        msg = conn.value()
        if msg[0] == "change":
            seen.append(msg)
        elif msg[0] == "resync":
            return seen, msg
    return seen, None


def main():
    data = tempfile.mkdtemp(prefix="ruvio-cdc-")
    try:
        # 1. no loss or repeats: write, read part, kill the server, reopen, continue from the cursor.
        proc = start(data, retain=64 * 1024 * 1024, segment=1 << 20)
        sub = Conn()
        start_cursor = sub.call("CHANGES", "SUBSCRIBE", "FROM", "NOW")
        w = Conn()
        for i in range(300):
            assert w.call("SET", f"k{i}", f"v{i}") == "OK"
        first, _ = events(sub, 120)
        cursor = first[-1][1]
        assert [e[4] for e in first] == [f"k{i}" for i in range(120)], "order broken"
        assert start_cursor == "0:0", start_cursor
        sub.s.close()
        stop(proc)

        proc = start(data, retain=64 * 1024 * 1024, segment=1 << 20)
        w = Conn()
        for i in range(300, 330):
            assert w.call("SET", f"k{i}", f"v{i}") == "OK"
        sub = Conn()
        assert sub.call("CHANGES", "SUBSCRIBE", "FROM", cursor, "VALUES") == cursor
        rest, _ = events(sub, 210)
        assert [e[4] for e in rest] == [f"k{i}" for i in range(120, 330)], "loss or repeat"
        assert rest[0][5] == "v120", "VALUES payload missing"
        # filtre
        f = Conn()
        f.call("CHANGES", "SUBSCRIBE", "FROM", "0:0", "MATCH", "k32*", "TYPES", "put")
        got, _ = events(f, 11)
        assert [e[4] for e in got] == ["k32"] + [f"k32{i}" for i in range(10)], got
        # a cursor ahead of the log is rejected
        assert Conn().call("CHANGES", "SUBSCRIBE", "FROM", "0:999999").startswith("ERR"), "cursor ahead"
        assert Conn().call("CHANGES", "SUBSCRIBE", "FROM", "7:1").startswith("ERR"), "wrong shard"
        print("ok: resume after restart, no loss or repeats, filter, error paths")
        stop(proc)

        # 2. bounded retention: small window + small segment, write load, WAL stays bounded, old cursor gets a resync.
        shutil.rmtree(data); os.makedirs(data)
        proc = start(data, retain=256 * 1024, segment=64 * 1024)
        w = Conn()
        for i in range(6000):
            w.call("SET", f"big{i % 50}", "x" * 200)
            if i % 1000 == 999:
                w.call("SAVE")
        wal = os.path.join(data, "wal")
        size = sum(os.path.getsize(os.path.join(dp, f)) for dp, _, fs in os.walk(data)
                   for f in fs if f.endswith(".wal"))
        assert size <= 256 * 1024 + 3 * 64 * 1024, f"WAL grew without bound: {size}"
        slow = Conn()
        slow.call("CHANGES", "SUBSCRIBE", "FROM", "0:1")
        _, resync = events(slow, 10 ** 9)
        assert resync and resync[0] == "resync", resync
        print(f"ok: WAL bounded at {size} bytes, lagging cursor got a resync: {resync[1]}")
        stop(proc)
    finally:
        shutil.rmtree(data, ignore_errors=True)


main()
