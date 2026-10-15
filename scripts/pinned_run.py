#!/usr/bin/env python3
"""Runs another benchmark script with its servers and load generator pinned to CPU sets (Linux).

    PIN_SERVER=0 PIN_CLIENT=2,4 python3 scripts/pinned_run.py scripts/redis_compare_features.py --ruvio ... --redis-server ...

Every `redis-server` / `ruvio` process the script starts gets `taskset -c $PIN_SERVER`; every `redis-benchmark`
gets `taskset -c $PIN_CLIENT`. The wrapped script is otherwise unchanged.
"""
import os, runpy, subprocess, sys

SERVER, CLIENT = os.environ.get("PIN_SERVER"), os.environ.get("PIN_CLIENT")
_popen = subprocess.Popen


def pin(argv):
    if not isinstance(argv, (list, tuple)) or not argv:
        return argv
    name = os.path.basename(str(argv[0]))
    if name == "redis-benchmark" and CLIENT:
        return ["taskset", "-c", CLIENT, *argv]
    if name in ("redis-server", "ruvio") and SERVER:
        return ["taskset", "-c", SERVER, *argv]
    return argv


class PinnedPopen(_popen):
    def __init__(self, args, *a, **kw):
        super().__init__(pin(args), *a, **kw)


subprocess.Popen = PinnedPopen
script = sys.argv[1]
sys.argv = sys.argv[1:]
sys.path.insert(0, os.path.dirname(os.path.abspath(script)))
runpy.run_path(script, run_name="__main__")
