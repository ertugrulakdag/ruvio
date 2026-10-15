#!/usr/bin/env python3
"""A hash whose every field has expired stays visible (TYPE hash, EXISTS 1, WRONGTYPE for list
commands) until the maintenance sweep reaches it. The sweep removes at most
`active_expire_keys` (16) hashes per 100 ms tick, so with a few thousand hashes due first the
key lingers for many seconds.

    python3 scripts/model_check_repros/hash_field_sweep_lag.py target/release-fast/ruvio [port]
"""

import socket
import sys
import time

from _servers import start


def encode(parts):
    return b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n%s\r\n" % (len(p), p) for p in parts)


def call(sock, *parts):
    sock.sendall(encode(list(parts)))
    data = b""
    while not data.endswith(b"\r\n"):
        data += sock.recv(4096)
    return data.strip()


BIN = sys.argv[1]
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 27231
HASHES = 3000
start(BIN, "server", PORT, '[persistence]\nprofile = "memory"\ndata_dir = "DATA_DIR"\n')
loader = socket.create_connection(("127.0.0.1", PORT))
batch = b"".join(encode([b"HSET", b"h%d" % i, b"f", b"v"])
                 + encode([b"HPEXPIRE", b"h%d" % i, b"100", b"FIELDS", b"1", b"f"]) for i in range(HASHES))
batch += encode([b"HSET", b"last", b"f", b"v"]) + encode([b"HPEXPIRE", b"last", b"150", b"FIELDS", b"1", b"f"])
loader.sendall(batch)
expired_at = time.time() + 0.15
time.sleep(0.5)
client = socket.create_connection(("127.0.0.1", PORT))
print("0.35 s after its only field expired:", "TYPE", call(client, b"TYPE", b"last"),
      "EXISTS", call(client, b"EXISTS", b"last"), "LPUSH", call(client, b"LPUSH", b"last", b"x"))

# A missing key takes the push (a fixed server); a visible dead hash refuses it.
while call(client, b"LPUSH", b"last", b"x").startswith(b"-WRONGTYPE"):
    time.sleep(0.05)
lag = time.time() - expired_at
print(f"'last' was visible for {lag:.1f} s after its only field expired")
sys.exit(1 if lag > 1.0 else 0)
