"""Live single-shard expiry probe: python3 examples/07_expiry_live.py target/release/ruvio."""

import argparse
import socket
import statistics
import subprocess
import time


def resp(*args):
    return b"*" + str(len(args)).encode() + b"\r\n" + b"".join(
        b"$" + str(len(arg)).encode() + b"\r\n" + arg + b"\r\n" for arg in args
    )


def metrics(client, reader, pid):
    client.sendall(resp(b"INFO", b"prometheus"))
    length = reader.readline()
    if not length.startswith(b"$"):
        raise RuntimeError(f"Unexpected INFO reply: {length!r}")
    text = reader.read(int(length[1:])).decode()
    if reader.read(2) != b"\r\n":
        raise RuntimeError("Incomplete INFO reply")

    def find(name, label=""):
        return int(next(
            line.rsplit(" ", 1)[1]
            for line in text.splitlines()
            if line.startswith(name + "{") and label in line.split("}")[0]
        ))

    rss = int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)]).strip()) * 1024
    return find("ruvio_expired_keys_total", 'reason="active"'), find("ruvio_keys"), rss


def writes(client, reader, prefix, count, ttl):
    for start in range(0, count, 1000):
        end = min(start + 1000, count)
        client.sendall(b"".join(
            resp(b"SET", f"{prefix}{index:06d}".encode(), b"x" * 64, b"PX", str(ttl).encode())
            for index in range(start, end)
        ))
        for _ in range(start, end):
            if reader.readline() != b"+OK\r\n":
                raise RuntimeError("Unexpected SET reply")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument("--future", type=int, default=6000)
    parser.add_argument("--due", type=int, default=6000)
    parser.add_argument("--budget", type=int, default=16)
    args = parser.parse_args()
    if args.future < 1 or args.due < 1:
        parser.error("--future and --due must be positive")

    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        port = reserved.getsockname()[1]
    server = subprocess.Popen(
        [args.binary, "--address", f"127.0.0.1:{port}", "--durability", "memory",
         "--shards", "1", "--active-expire-keys", str(args.budget)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        for _ in range(100):
            try:
                client = socket.create_connection(("127.0.0.1", port), timeout=1)
                break
            except OSError:
                if server.poll() is not None:
                    raise RuntimeError(f"Server exited with status {server.returncode}")
                time.sleep(0.05)
        else:
            raise RuntimeError("Server did not start")
        with client:
            client.settimeout(10)
            with client.makefile("rb") as reader:
                writes(client, reader, "future", args.future, 600_000)
                writes(client, reader, "due", args.due, 4_000)
                expired, keys, rss = metrics(client, reader, server.pid)
                print(f"loaded keys={keys} active_expired={expired} rss_bytes={rss}", flush=True)
                if expired != 0:
                    raise RuntimeError("Keys expired during loading; reduce --due")
                time.sleep(4.3)
                start = time.monotonic()
                latencies = []
                for _ in range(100):
                    began = time.perf_counter_ns()
                    client.sendall(resp(b"PING"))
                    if reader.readline() != b"+PONG\r\n":
                        raise RuntimeError("Unexpected PING reply")
                    latencies.append((time.perf_counter_ns() - began) / 1000)
                    time.sleep(0.008)
                expired, keys, rss = metrics(client, reader, server.pid)
                print(f"after_pings active_expired={expired} keys={keys} rss_bytes={rss} "
                      f"ping_p50_us={statistics.median(latencies):.1f} "
                      f"ping_p99_us={sorted(latencies)[98]:.1f}", flush=True)
                while time.monotonic() - start < 95 and expired < args.due:
                    time.sleep(0.3)
                    expired, keys, rss = metrics(client, reader, server.pid)
                print(f"done_s={time.monotonic() - start:.2f} "
                      f"active_expired={expired} keys={keys} rss_bytes={rss}", flush=True)
                if expired < args.due:
                    raise RuntimeError("Expiry did not drain within 95 seconds")
    finally:
        server.terminate()
        try:
            server.wait(timeout=3)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()


if __name__ == "__main__":
    main()
