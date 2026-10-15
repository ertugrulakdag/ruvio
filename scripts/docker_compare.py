#!/usr/bin/env python3
"""redis_compare_features.py against Docker containers (host network, server cpuset 1).

    python3 scripts/docker_compare.py --ruvio-image IMG --redis-image IMG [redis_compare_features options]
"""
import argparse, subprocess, sys, time
sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.abspath(__file__)))
import redis_compare as rc
import redis_compare_features as rf

pre = argparse.ArgumentParser(add_help=False)
pre.add_argument("--ruvio-image", required=True)
pre.add_argument("--redis-image", required=True)
known, rest = pre.parse_known_args()
sys.argv = [sys.argv[0]] + rest + ["--ruvio", "/bin/true"]  # the binary check in main() needs an existing path


class DockerServer:
    def __init__(self, name, image, port, args):
        self.name, self.port = f"cmp-{name}-{port}", port
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)
        cmd = ["docker", "run", "-d", "--rm", "--network", "host", "--cpuset-cpus", "1", "--name", self.name]
        if name == "ruvio":
            cmd += ["--tmpfs", "/data:rw,uid=10001,gid=10001"]
        subprocess.run(cmd + [image] + args, check=True, capture_output=True)
        rc.wait_ping(port)
        self.pid = int(subprocess.run(["docker", "inspect", "-f", "{{.State.Pid}}", self.name], capture_output=True, text=True).stdout)

    def stop(self):
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)

    def rss_kib(self):
        for line in open(f"/proc/{self.pid}/status"):
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
        return 0


def start(args, name, workdir):
    if name == "ruvio":
        return DockerServer(name, known.ruvio_image, args.ruvio_port, ["--address", f"127.0.0.1:{args.ruvio_port}", "--durability", "memory", "--shards", "1", "--data-dir", "/data"])
    return DockerServer(name, known.redis_image, args.redis_port, ["redis-server", "--bind", "127.0.0.1", "--port", str(args.redis_port), "--save", "", "--appendonly", "no"])


rc.start = start
rf.rc.start = start
rf.main()
