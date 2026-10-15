"""Shared helpers for the model checker repros: start ruvio processes on local ports in a
temporary directory and stop them on exit."""

import atexit
import os
import shutil
import socket
import subprocess
import tempfile
import time

_children = []
ROOT = tempfile.mkdtemp(prefix="ruvio-repro-")
atexit.register(lambda: [c.kill() for c in _children if c.poll() is None])
atexit.register(lambda: shutil.rmtree(ROOT, ignore_errors=True))


def start(binary, name, port, toml, env=None):
    """Writes `toml` (without address/data_dir) for server `name` and starts it on `port`."""

    directory = os.path.join(ROOT, name)
    os.makedirs(directory, exist_ok=True)
    config = os.path.join(directory, "ruvio.toml")

    with open(config, "w") as handle:
        handle.write(f'address = "127.0.0.1:{port}"\nmetrics_address = ""\n'
                     + toml.replace("DATA_DIR", os.path.join(directory, "data")))
    process = subprocess.Popen([binary, "--config", config], env=dict(os.environ, **(env or {})),
                               stdout=open(os.path.join(directory, "log"), "a"), stderr=subprocess.STDOUT)
    _children.append(process)

    for _ in range(400):
        if process.poll() is not None:
            return process
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            return process
        except OSError:
            time.sleep(0.05)
    return process


def last_log_line(name):
    with open(os.path.join(ROOT, name, "log")) as handle:
        lines = handle.read().strip().splitlines()
    return lines[-1] if lines else ""
