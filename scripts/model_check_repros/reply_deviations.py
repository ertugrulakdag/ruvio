#!/usr/bin/env python3
"""Replies that differ from Redis, found by the model checker.

1. A command refused with NOPERM while queued in MULTI does not abort the transaction: EXEC
   runs the other queued commands (Redis answers EXECABORT and runs nothing).
2. RPUSH to a key a client is blocked on with BLPOP replies 0; Redis replies with the list
   length after the push (1).
3. A write whose WAL record is larger than wal_segment_bytes gets "MISCONF persistence error"
   (the write is refused and nothing is logged), which clients read as a disk failure. An
   error that names the limit passes.

    python3 scripts/model_check_repros/reply_deviations.py target/release-fast/ruvio [port]
"""

import sys
import threading
import time

import redis

from _servers import start

BIN = sys.argv[1]
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 27241
start(BIN, "server", PORT, '[persistence]\nprofile = "everysec"\ndata_dir = "DATA_DIR"\n'
      'wal_segment_bytes = 65536\n[[security.users]]\nname = "limited"\n'
      'rules = "on >secret ~own:* +@all"\n')
admin = redis.Redis(port=PORT)
limited = redis.Redis(port=PORT, username="limited", password="secret")
failures = 0

pipe = limited.pipeline(transaction=True)
pipe.set("own:a", "1")
pipe.set("other:a", "1")
try:
    print("1. EXEC replied", pipe.execute(raise_on_error=False))
except redis.exceptions.ResponseError as error:
    print("1. EXEC replied", error)
ran = admin.exists("own:a")
print(f"1. {'FAIL' if ran else 'ok'}: own:a {'was' if ran else 'was not'} written by a transaction "
      "that queued a refused command")
failures += ran

box = {}
blocker = threading.Thread(target=lambda: box.setdefault("pop", admin.blpop("q", 2)))
blocker.start()
time.sleep(0.2)
pushed = redis.Redis(port=PORT).rpush("q", "x")
blocker.join()
print(f"2. {'ok' if pushed == 1 else 'FAIL'}: RPUSH replied {pushed} while BLPOP waited (Redis: 1); "
      f"BLPOP got {box['pop']}")
failures += pushed != 1

try:
    admin.set("big", "x" * 70_000)
    print("3. ok: the large SET was accepted")
except redis.exceptions.ResponseError as error:
    # Refusing the write is fine when the error names the limit, not a disk failure.
    clear = "wal_segment_bytes" in str(error)
    print(f"3. {'ok' if clear else 'FAIL'}: a SET larger than wal_segment_bytes replied \"{error}\"; "
          f"persistence state stays {admin.info('persistence')['ruvio_persistence_state']!r}")
    failures += not clear
sys.exit(1 if failures else 0)
