#!/usr/bin/env python3
"""A replica killed with -9 while frames arrive replays some of them twice.

A primary and a replica (`always`, background checkpoints every 100 ms). The primary
appends to 50 lists while the replica is killed with -9 and restarted. Once the replica has
caught up, every list must be as long as the primary's. On the affected build the replica's
lists hold extra elements (frames applied twice at startup) in some trials; a stream append
applied twice stops startup with "persistence corruption: stream append replay failed".

    python3 scripts/model_check_repros/replica_checkpoint_replay.py target/release-fast/ruvio [port]
"""

import signal
import sys
import time

import redis

from _servers import last_log_line, start

BIN = sys.argv[1]
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 27161
COMMON = ('[persistence]\nprofile = "always"\ndata_dir = "DATA_DIR"\nsnapshot_interval_ms = 100\n'
          'snapshot_journal_bytes = 4096\n[replication]\n')
PRIMARY = COMMON + f'role = "primary"\nlisten = "127.0.0.1:{PORT + 20}"\n'
REPLICA = COMMON + f'role = "replica"\nprimary = "127.0.0.1:{PORT + 20}"\n'

start(BIN, "primary", PORT, PRIMARY)
replica_process = start(BIN, "replica", PORT + 10, REPLICA)
primary = redis.Redis(port=PORT)
pipe = primary.pipeline(transaction=False)

for i in range(100_000):  # enough keys that a checkpoint walk spans several slices
    pipe.set(f"fill:{i}", "x" * 32)
pipe.execute()
time.sleep(2)
lists = [f"list:{i}" for i in range(50)]
rounds = 0


def write_for(seconds):
    global rounds
    end = time.time() + seconds

    while time.time() < end:
        pipe = primary.pipeline(transaction=False)
        for key in lists:
            pipe.rpush(key, rounds)
        pipe.execute()
        rounds += 1
        time.sleep(0.005)


def differing(timeout=30):
    want = [primary.llen(key) for key in lists]
    deadline = time.time() + timeout

    while True:
        try:
            replica = redis.Redis(port=PORT + 10)
            got = [replica.llen(key) for key in lists]
        except (redis.exceptions.ResponseError, redis.exceptions.ConnectionError) as error:
            got = [f"error: {error}"] * len(lists)
        bad = [(key, w, g) for key, w, g in zip(lists, want, got) if w != g]

        if not bad or time.time() > deadline:
            return bad
        time.sleep(0.2)


failures = 0

for trial in range(12):
    write_for(1.0 + 0.25 * (trial % 4))
    replica_process.send_signal(signal.SIGKILL)  # while frames and checkpoints are in flight
    replica_process.wait()
    replica_process = start(BIN, "replica", PORT + 10, REPLICA)

    if replica_process.poll() is not None:
        print(f"trial {trial}: FAIL the replica did not start again: {last_log_line('replica')[-200:]}")
        sys.exit(1)
    write_for(0.5)
    bad = differing()
    failures += bool(bad)
    print(f"trial {trial}: {'FAIL' if bad else 'ok'} {len(bad)} lists differ from the primary {bad[:3]}")
sys.exit(1 if failures else 0)
