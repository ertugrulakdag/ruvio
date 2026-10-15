#!/usr/bin/env python3
"""SAVE with several shards waits (up to 120 s) while a checkpoint file cannot be written;
with one shard it fails at once with MISCONF.

Uses the model checker's LD_PRELOAD shim (compiled here) to make writes under
`<data>/checkpoints/` fail with ENOSPC, sends SAVE, and reports how long the reply took.

    python3 scripts/model_check_repros/save_waits_with_shards.py target/release-fast/ruvio [port]
"""

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

from _servers import ROOT, start

BIN = sys.argv[1]
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 27221
shim = os.path.join(ROOT, "shim.so")
subprocess.run(["cc", "-O2", "-shared", "-fPIC", "-o", shim,
                str(Path(__file__).resolve().parent.parent / "model_check_shim.c"), "-ldl", "-lpthread"], check=True)
failed = False

for shards in (1, 2):
    name = f"shards{shards}"
    control = os.path.join(ROOT, f"{name}.fault")
    process = start(BIN, name, PORT, f'shards = {shards}\n[persistence]\nprofile = "snapshot"\n'
                    'data_dir = "DATA_DIR"\nsnapshot_interval_ms = 0\n',
                    env={"LD_PRELOAD": shim, "RUVIO_FAULT_CONTROL": control})
    sock = socket.create_connection(("127.0.0.1", PORT))
    sock.settimeout(5)
    sock.sendall(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
    sock.recv(64)

    with open(control, "w") as handle:
        handle.write(f"write ENOSPC {os.path.join(ROOT, name, 'data', 'checkpoints')}/\n")
    time.sleep(0.01)
    started = time.time()
    sock.sendall(b"*1\r\n$4\r\nSAVE\r\n")

    try:
        reply = sock.recv(200)
        print(f"shards={shards}: SAVE replied {reply!r} after {time.time() - started:.2f} s")
    except socket.timeout:
        failed = True
        print(f"shards={shards}: FAIL SAVE got no reply within 5 s while the disk is full")
    os.unlink(control)
    process.kill()
    process.wait()
sys.exit(1 if failed else 0)
