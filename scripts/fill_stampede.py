#!/usr/bin/env python3
"""FILLGET single-flight check: many clients miss one key, exactly one is told to fill."""

import argparse
import socket
import threading
import time

from resp_conformance import encode, read_response


def connect(host, port):
    connection = socket.create_connection((host, port), timeout=10)
    return connection, connection.makefile("rwb", buffering=0)


def call(stream, *command):
    stream.write(encode(*command))
    stream.flush()
    return read_response(stream)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--clients", type=int, default=1000)
    args = parser.parse_args()

    key = b"stampede:key"
    clients = [connect(args.host, args.port) for _ in range(args.clients)]
    call(clients[0][1], b"DEL", key)
    results = [None] * args.clients
    gate = threading.Barrier(args.clients)

    def worker(index):
        gate.wait()
        results[index] = call(clients[index][1], b"FILLGET", key, b"5000")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(args.clients)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    statuses = [reply[1][0][1] for reply in results]
    fills = [i for i, status in enumerate(statuses) if status == b"FILL"]
    assert len(fills) == 1, f"expected one FILL, got {len(fills)}"
    assert statuses.count(b"BUSY") == args.clients - 1, statuses.count(b"BUSY")
    token = results[fills[0]][1][1][1]

    stream = clients[0][1]
    assert call(stream, b"FILLSET", key, b"v", b"60000", b"999999") == (b":", 0), "wrong token must be refused"
    assert call(stream, b"FILLSET", key, b"v", b"60000", token) == (b":", 1)
    assert call(stream, b"FILLSET", key, b"w", b"60000", token) == (b":", 0), "token is single use"
    assert call(stream, b"FILLGET", key, b"5000")[1][0][1] == b"HIT"
    print(f"ok: 1 FILL, {args.clients - 1} BUSY, token fenced, HIT after fill")

    # WAIT: the same stampede, but the losers are held until the filler writes.
    wait_key = b"stampede:wait"
    call(stream, b"DEL", wait_key)
    waits = [None] * args.clients
    gate = threading.Barrier(args.clients)

    def waiter(index):
        gate.wait()
        waits[index] = call(clients[index][1], b"FILLGET", wait_key, b"5000", b"WAIT", b"3000")

    threads = [threading.Thread(target=waiter, args=(i,)) for i in range(args.clients)]
    for thread in threads:
        thread.start()
    # Only the filler answers at once; the rest stay parked until FILLSET.
    deadline = time.time() + 5
    while time.time() < deadline and sum(reply is not None for reply in waits) < 1:
        time.sleep(0.01)
    filler = next(i for i, reply in enumerate(waits) if reply is not None)
    assert waits[filler][1][0][1] == b"FILL", waits[filler]
    time.sleep(0.2)
    assert sum(reply is not None for reply in waits) == 1, "waiters must stay parked"
    assert call(clients[filler][1], b"FILLSET", wait_key, b"v", b"60000", waits[filler][1][1][1]) == (b":", 1)
    for thread in threads:
        thread.join()
    hits = sum(1 for i, reply in enumerate(waits) if i != filler and reply[1][0][1] == b"HIT")
    assert hits == args.clients - 1, f"expected {args.clients - 1} HIT, got {hits}"
    print(f"ok: WAIT held {hits} callers until FILLSET, all HIT")


if __name__ == "__main__":
    main()
