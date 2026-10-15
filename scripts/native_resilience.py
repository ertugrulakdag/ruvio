#!/usr/bin/env python3
"""Run short native-process crash/recovery checks on macOS, Linux, or Windows."""

import argparse
import datetime as dt
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

from resp_conformance import encode, read_response


ROOT = Path(__file__).resolve().parents[1]
VALUE = b"native-crash-resilience" * 4
PIPELINE = 16


def run(command, *, timeout=180):
    result = subprocess.run(
        command, cwd=ROOT, text=True, capture_output=True, timeout=timeout
    )
    if result.returncode:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(map(str, command))}\n"
            f"{result.stdout}\n{result.stderr}"
        )
    return result.stdout.strip()


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def wait_ready(port, process, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Ruvio exited during startup with {process.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2) as sock:
                sock.sendall(encode(b"PING"))
                if read_response(sock.makefile("rb")) == (b"+", b"PONG"):
                    return
        except (OSError, AssertionError):
            time.sleep(0.05)
    raise TimeoutError("Ruvio did not become ready")


def start_server(binary, data_dir, port, durability, log_path):
    config_path = data_dir.parent / "ruvio.toml"
    config_path.write_text("", encoding="utf-8")
    log = log_path.open("ab")
    process = subprocess.Popen(
        [
            str(binary),
            "--config",
            str(config_path),
            "--address",
            f"127.0.0.1:{port}",
            "--durability",
            durability,
            "--data-dir",
            str(data_dir),
            "--wal-segment-bytes",
            "4096",
            "--protected-mode",
            "false",
        ],
        cwd=ROOT,
        env={**os.environ, "RUVIO_ALLOW_UNAUTHENTICATED": "true"},
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    try:
        wait_ready(port, process)
    except Exception:
        process.kill()
        process.wait(timeout=5)
        log.close()
        raise RuntimeError(
            f"Ruvio startup failed:\n{log_path.read_text(errors='replace')[-4000:]}"
        )
    return process, log


def crash_trial(binary, root, durability, trial, runtime_ms, max_writes):
    data_dir = root / f"{durability}-{trial}" / "data"
    data_dir.mkdir(parents=True)
    log_path = data_dir.parent / "ruvio.log"
    port = free_port()
    process, log = start_server(binary, data_dir, port, durability, log_path)
    acknowledged = 0
    sent = 0
    write_error = None
    finish = threading.Event()

    def writer():
        nonlocal acknowledged, sent, write_error
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
                sock.settimeout(0.1)
                stream = sock.makefile("rb")
                while not finish.is_set() and sent < max_writes:
                    count = min(PIPELINE, max_writes - sent)
                    batch = b"".join(
                        encode(
                            b"SET",
                            f"crash:{sent + index:08d}".encode(),
                            VALUE,
                        )
                        for index in range(count)
                    )
                    sock.sendall(batch)
                    sent += count
                    for _ in range(count):
                        try:
                            response = read_response(stream)
                        except (OSError, TimeoutError, AssertionError) as error:
                            if not finish.is_set():
                                write_error = str(error)
                            return
                        if response != (b"+", b"OK"):
                            write_error = repr(response)
                            return
                        acknowledged += 1
        except (OSError, TimeoutError) as error:
            if not finish.is_set():
                write_error = str(error)

    worker = threading.Thread(target=writer, daemon=True)
    try:
        worker.start()
        time.sleep(runtime_ms / 1000)
        process.kill()
        process.wait(timeout=10)
        finish.set()
        worker.join(timeout=5)
        if worker.is_alive():
            raise TimeoutError("writer thread did not stop after the server crash")
        log.close()

        recovered, recovery_log = start_server(
            binary, data_dir, port, durability, log_path
        )
        try:
            present = []
            with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
                stream = sock.makefile("rwb")
                for start in range(0, sent, 32):
                    keys = [
                        f"crash:{index:08d}".encode()
                        for index in range(start, min(start + 32, sent))
                    ]
                    stream.write(encode(b"MGET", *keys))
                    stream.flush()
                    reply = read_response(stream)
                    if reply[0] != b"*" or reply[1] is None:
                        raise AssertionError(f"unexpected MGET response: {reply!r}")
                    present.extend(value is not None for _, value in reply[1])
            first_missing = next(
                (index for index, exists in enumerate(present) if not exists), sent
            )
            later = next(
                (index for index in range(first_missing + 1, sent) if present[index]),
                None,
            )
            if later is not None:
                raise AssertionError(
                    f"recovery contained a hole: key {first_missing} missing, "
                    f"key {later} present"
                )
            if durability == "always" and first_missing < acknowledged:
                raise AssertionError(
                    f"always lost acknowledged key {first_missing}; "
                    f"acknowledged through {acknowledged - 1}"
                )
            return {
                "acknowledged_writes": acknowledged,
                "durability": durability,
                "first_missing_key": first_missing if first_missing < sent else None,
                "recovered_keys": sum(present),
                "sent_writes": sent,
                "result": "passed",
                "writer_error": write_error,
            }
        finally:
            recovered.terminate()
            try:
                recovered.wait(timeout=10)
            except subprocess.TimeoutExpired:
                recovered.kill()
                recovered.wait(timeout=5)
            recovery_log.close()
    except Exception:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        if not log.closed:
            log.close()
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary",
        type=Path,
        help="existing Ruvio binary (defaults to target/release-fast/ruvio[.exe])",
    )
    parser.add_argument("--build", action="store_true", help="build the release-fast binary first")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--runtime-ms", type=int, default=150)
    parser.add_argument("--max-writes", type=int, default=5000)
    parser.add_argument(
        "--durability",
        nargs="+",
        choices=("everysec", "always"),
        default=("everysec", "always"),
    )
    args = parser.parse_args()
    if args.trials < 1 or args.runtime_ms < 1 or args.max_writes < PIPELINE:
        parser.error("--trials, --runtime-ms, and --max-writes must be positive and bounded")
    if args.build:
        run(["cargo", "build", "--locked", "--profile", "release-fast", "--bin", "ruvio"])
    binary = args.binary or (
        ROOT / "target" / "release-fast" / ("ruvio.exe" if os.name == "nt" else "ruvio")
    )
    if not binary.is_file():
        parser.error(f"Ruvio binary not found: {binary}; pass --build or --binary")
    if not shutil.which("python") and not shutil.which("python3"):
        parser.error("Python is required to run the RESP crash/recovery checks")

    results = {}
    failures = 0
    with tempfile.TemporaryDirectory(prefix="ruvio-native-resilience-") as temp:
        root = Path(temp)
        for durability in args.durability:
            results[durability] = []
            for index in range(args.trials):
                try:
                    trial = crash_trial(
                        binary,
                        root,
                        durability,
                        index + 1,
                        random.randint(args.runtime_ms // 2, args.runtime_ms * 2),
                        args.max_writes,
                    )
                    results[durability].append(trial)
                    print(
                        f"{durability} trial {index + 1}: passed "
                        f"(acked={trial['acknowledged_writes']}, "
                        f"recovered={trial['recovered_keys']}, sent={trial['sent_writes']})"
                    )
                except Exception as error:
                    failures += 1
                    results[durability].append(
                        {"error": str(error), "result": "failed"}
                    )
                    print(f"{durability} trial {index + 1}: failed: {error}", file=sys.stderr)

    report = {
        "created_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "platform": sys.platform,
        "binary": str(binary),
        "boundary": "Native process-crash tests do not simulate host power loss.",
        "configuration": {
            "trials": args.trials,
            "runtime_ms": args.runtime_ms,
            "max_writes_per_trial": args.max_writes,
        },
        "results": results,
    }
    output_dir = ROOT / "target" / "native-resilience"
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / (
        dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + f"-{os.getpid()}.json"
    )
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Report: {report_path.relative_to(ROOT)}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
