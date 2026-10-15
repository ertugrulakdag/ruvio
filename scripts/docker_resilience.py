#!/usr/bin/env python3
"""Manually inject bounded filesystem and process faults into isolated Ruvio containers."""

import argparse
import datetime as dt
import json
import os
import random
import shlex
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

from resp_conformance import encode, read_response


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_IMAGE = "ruvio:resilience-test"
DATA_SIZE = 32 * 1024 * 1024
WAL_SEGMENT_BYTES = 4096
VALUE = b"docker-resilience-value-" * 48


def run(command, *, timeout=30, check=True):
    result = subprocess.run(
        command, text=True, capture_output=True, timeout=timeout, check=False
    )
    if check and result.returncode:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(command)}\n"
            f"{result.stdout}\n{result.stderr}"
        )
    return result


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def connect(port, timeout=2):
    connection = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    connection.settimeout(timeout)
    return connection.makefile("rwb")


def request(stream, *arguments):
    stream.write(encode(*arguments))
    stream.flush()
    return read_response(stream)


class DockerCase:
    def __init__(self, image, data_size=DATA_SIZE, network=None, alias=None):
        self.image = image
        self.name = f"ruvio-resilience-{os.getpid()}-{random.randrange(1 << 32):08x}"
        self.label = f"ruvio.resilience.owner={self.name}"
        self.container_id = None
        self.port = free_port()
        self._start(data_size, network, alias)

    def docker(self, *args, timeout=30, check=True):
        return run(["docker", *args], timeout=timeout, check=check)

    def _start(self, data_size, network, alias):
        command = [
            "run",
            "--detach",
            "--name",
            self.name,
            "--label",
            self.label,
            "--user",
            "10001:10001",
        ]
        if network:
            command.extend(["--network", network])
        if alias:
            command.extend(["--network-alias", alias])
        command.extend([
            "--publish",
            f"127.0.0.1:{self.port}:6379",
            "--tmpfs",
            f"/data:rw,size={data_size},uid=10001,gid=10001,mode=0755",
            "--env",
            "RUVIO_CONFIG=/tmp/ruvio.toml",
            "--env",
            "RUVIO_ALLOW_UNAUTHENTICATED=true",
            "--entrypoint",
            "/bin/sh",
            self.image,
            "-c",
            "while :; do sleep 3600; done",
        ])
        self.container_id = self.docker(*command).stdout.strip()

    def exec(self, *args, timeout=30, check=True, user=None):
        command = ["exec"]
        if user:
            command.extend(["--user", user])
        command.extend([self.container_id, *args])
        return self.docker(*command, timeout=timeout, check=check)

    def start_server(self, config=None, durability="always"):
        if config is not None:
            self.exec(
                "/bin/sh",
                "-c",
                f"printf %s {shlex.quote(config)} >/tmp/ruvio.toml",
            )
        command = (
            "rm -f /tmp/ruvio.pid; "
            "/usr/local/bin/ruvio --config /tmp/ruvio.toml "
            f"--address 0.0.0.0:6379 --durability {durability} --data-dir /data "
            f"--wal-segment-bytes {WAL_SEGMENT_BYTES} --protected-mode false "
            ">/tmp/ruvio.log 2>&1 & echo $! >/tmp/ruvio.pid"
        )
        self.exec("/bin/sh", "-c", command)
        deadline = time.monotonic() + 30
        last_error = None
        while time.monotonic() < deadline:
            try:
                with connect(self.port) as stream:
                    if request(stream, b"PING") == (b"+", b"PONG"):
                        started = self.exec(
                            "grep",
                            "-q",
                            "server listening",
                            "/tmp/ruvio.log",
                            check=False,
                        )
                        if started.returncode == 0:
                            return
                        last_error = "PING reached a listener without a fresh startup log"
            except (OSError, AssertionError) as error:
                last_error = error
            time.sleep(0.2)
        logs = self.logs()
        raise RuntimeError(f"Ruvio did not become ready: {last_error}\n{logs}")

    def stop_server(self, signal):
        self.exec(
            "/bin/sh",
            "-c",
            f"kill -{signal} \"$(cat /tmp/ruvio.pid)\"",
            check=False,
        )
        if signal != 9:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                result = self.exec(
                    "/bin/sh",
                    "-c",
                    "kill -0 \"$(cat /tmp/ruvio.pid)\" 2>/dev/null",
                    check=False,
                )
                if result.returncode:
                    break
                time.sleep(0.1)

    def logs(self):
        result = self.exec("cat", "/tmp/ruvio.log", check=False)
        return result.stdout[-12000:]

    def storage_state(self):
        disk = self.exec("df", "-k", "/data", check=False).stdout
        wal = self.exec(
            "/bin/sh", "-c", "ls -la /data/wal/shard-0000", check=False
        ).stdout
        return f"df:\n{disk}\nWAL directory:\n{wal}"

    def close(self):
        if self.container_id:
            self.docker("rm", "--force", self.container_id, check=False)


def fill_until_error(case, limit):
    acknowledged = []
    failure = None
    started = time.monotonic()
    with connect(case.port, timeout=30) as stream:
        for index in range(limit):
            key = f"resilience:{index:08d}".encode()
            response = request(stream, b"SET", key, VALUE)
            if response == (b"+", b"OK"):
                acknowledged.append(key)
                continue
            failure = repr(response)
            break
    return {
        "acknowledged_writes": len(acknowledged),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "failure_response": failure,
        "keys": acknowledged,
    }


def verify_keys(case, keys):
    verified = 0
    with connect(case.port) as stream:
        for offset in range(0, len(keys), 32):
            batch = keys[offset : offset + 32]
            response = request(stream, b"MGET", *batch)
            expected = (b"*", [(b"$", VALUE) for _ in batch])
            if response != expected:
                raise AssertionError(
                    f"recovery mismatch in key batch at {offset}: "
                    f"expected {len(batch)} values, got {response!r}"
                )
            verified += len(batch)
    return verified


def disk_full(args):
    case = DockerCase(DEFAULT_IMAGE)
    try:
        case.start_server()
        case.exec(
            "dd",
            "if=/dev/zero",
            "of=/data/.reserve",
            "bs=1M",
            "count=20",
            "status=none",
            user="0:0",
        )
        result = fill_until_error(case, limit=args.max_writes)
        if not result["failure_response"] or not result["failure_response"].startswith(
            "(b'-',"
        ):
            raise AssertionError("writes did not fail before the safety limit")
        fault_logs = case.logs()
        with connect(case.port) as stream:
            blocked_write = request(stream, b"SET", b"after-full", VALUE)
        if blocked_write == (b"+", b"OK"):
            raise AssertionError(
                "server acknowledged a write after the full-disk error; "
                f"first failure was {result['failure_response']}, "
                f"subsequent reply was {blocked_write!r}\n{case.storage_state()}"
            )
        case.exec("rm", "-f", "/data/.reserve", user="0:0")
        case.stop_server(15)
        case.start_server()
        verified = verify_keys(case, result["keys"])
        recovery_logs = case.logs()
        return {
            key: value
            for key, value in result.items()
            if key != "keys"
        } | {
            "blocked_write_response": repr(blocked_write),
            "fault_logs": fault_logs,
            "recovered_acknowledged_writes": verified,
            "recovery_logs": recovery_logs,
            "result": "passed",
        } | {"health_check": "PING returned PONG"}
    except Exception as error:
        raise RuntimeError(f"{error}\nRuvio logs:\n{case.logs()}") from error
    finally:
        case.close()


def permission_denied(args):
    case = DockerCase(DEFAULT_IMAGE)
    try:
        case.start_server()
        with connect(case.port) as stream:
            if request(stream, b"SET", b"before-permission-loss", VALUE) != (
                b"+",
                b"OK",
            ):
                raise AssertionError("could not seed the WAL before permission change")
        case.exec(
            "chmod", "0555", "/data/wal/shard-0000", user="0:0"
        )
        result = fill_until_error(case, limit=args.max_writes)
        if not result["failure_response"] or not result["failure_response"].startswith(
            "(b'-',"
        ):
            raise AssertionError(
                "WAL rotation did not fail with a RESP error; "
                f"observed {result['failure_response']}"
            )
        fault_logs = case.logs()
        with connect(case.port) as stream:
            blocked_write = request(stream, b"SET", b"after-permission-loss", VALUE)
        if blocked_write == (b"+", b"OK"):
            raise AssertionError(
                f"server acknowledged a write after WAL error {result['failure_response']}; "
                f"subsequent reply was {blocked_write!r}\n{case.storage_state()}"
            )
        case.exec(
            "chmod", "0755", "/data/wal/shard-0000", user="0:0"
        )
        case.stop_server(15)
        case.start_server()
        verified = verify_keys(case, [b"before-permission-loss", *result["keys"]])
        recovery_logs = case.logs()
        return {
            "acknowledged_writes": result["acknowledged_writes"] + 1,
            "blocked_write_response": repr(blocked_write),
            "elapsed_seconds": result["elapsed_seconds"],
            "failure_response": result["failure_response"],
            "fault_logs": fault_logs,
            "recovered_acknowledged_writes": verified,
            "recovery_logs": recovery_logs,
            "result": "passed",
        } | {"health_check": "PING returned PONG"}
    except Exception as error:
        raise RuntimeError(f"{error}\nRuvio logs:\n{case.logs()}") from error
    finally:
        case.close()


def process_crash(_args):
    case = DockerCase(DEFAULT_IMAGE)
    try:
        case.start_server()
        keys = []
        with connect(case.port) as stream:
            for index in range(100):
                key = f"crash:{index:04d}".encode()
                if request(stream, b"SET", key, VALUE) != (b"+", b"OK"):
                    raise AssertionError(f"seed write {index} was not acknowledged")
                keys.append(key)
        case.stop_server(9)
        crash_logs = case.logs()
        case.start_server()
        verified = verify_keys(case, keys)
        recovery_logs = case.logs()
        return {
            "acknowledged_writes": len(keys),
            "crash_logs": crash_logs,
            "health_check": "PING returned PONG",
            "recovered_acknowledged_writes": verified,
            "recovery_logs": recovery_logs,
            "result": "passed",
        }
    except Exception as error:
        raise RuntimeError(f"{error}\nRuvio logs:\n{case.logs()}") from error
    finally:
        case.close()


def replica_promotion(_args):
    network_name = f"ruvio-resilience-net-{os.getpid()}-{random.randrange(1 << 32):08x}"
    label = f"ruvio.resilience.owner={network_name}"
    network_id = run(
        [
            "docker",
            "network",
            "create",
            "--label",
            label,
            network_name,
        ]
    ).stdout.strip()
    primary = None
    replica = None
    keys = [f"replication:{index:04d}".encode() for index in range(100)]
    partition_keys = [f"partition:{index:04d}".encode() for index in range(100)]
    try:
        primary = DockerCase(
            DEFAULT_IMAGE, network=network_name, alias="ruvio-resilience-primary"
        )
        replica = DockerCase(
            DEFAULT_IMAGE, network=network_name, alias="ruvio-resilience-replica"
        )
        primary.start_server(
            '[persistence]\nprofile = "always"\ndata_dir = "/data"\n'
            'wal_segment_bytes = 4096\n[replication]\nrole = "primary"\n'
            'listen = "0.0.0.0:26379"\n[security]\n'
            'replication_password = "resilience-secret"\n'
        )
        replica.start_server(
            '[persistence]\nprofile = "always"\ndata_dir = "/data"\n'
            'wal_segment_bytes = 4096\n[replication]\nrole = "replica"\n'
            'primary = "ruvio-resilience-primary:26379"\n[security]\n'
            'replication_password = "resilience-secret"\n'
        )
        with connect(primary.port) as stream:
            for key in keys:
                if request(stream, b"SET", key, VALUE) != (b"+", b"OK"):
                    raise AssertionError(f"primary did not acknowledge {key!r}")

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                verified = verify_keys(replica, keys)
                break
            except AssertionError:
                time.sleep(0.1)
        else:
            raise AssertionError("replica did not apply all acknowledged primary writes")

        run(["docker", "network", "disconnect", network_id, replica.container_id])
        with connect(primary.port) as stream:
            for key in partition_keys:
                if request(stream, b"SET", key, VALUE) != (b"+", b"OK"):
                    raise AssertionError(f"primary did not acknowledge partition write {key!r}")
        reconnect_started = time.monotonic()
        run(
            [
                "docker",
                "network",
                "connect",
                "--alias",
                "ruvio-resilience-replica",
                network_id,
                replica.container_id,
            ]
        )
        all_keys = keys + partition_keys
        reconnect_deadline = time.monotonic() + 30
        while time.monotonic() < reconnect_deadline:
            try:
                replica_applied = verify_keys(replica, all_keys)
                break
            except AssertionError:
                time.sleep(0.1)
        else:
            raise AssertionError("replica did not catch up after network partition")
        reconnect_seconds = round(time.monotonic() - reconnect_started, 3)

        primary_logs = primary.logs()
        primary.stop_server(9)
        replica.stop_server(15)
        replica.start_server(
            '[persistence]\nprofile = "always"\ndata_dir = "/data"\n'
            'wal_segment_bytes = 4096\n[replication]\nrole = "primary"\n'
            'listen = "0.0.0.0:26379"\n[security]\n'
            'replication_password = "resilience-secret"\n'
        )
        recovered = verify_keys(replica, all_keys)
        with connect(replica.port) as stream:
            promoted_write = request(stream, b"SET", b"promoted:write", VALUE)
        if promoted_write != (b"+", b"OK"):
            raise AssertionError(f"promoted replica rejected a write: {promoted_write!r}")
        return {
            "acknowledged_primary_writes": len(keys),
            "promoted_write_response": repr(promoted_write),
            "partition_acknowledged_writes": len(partition_keys),
            "replica_applied_after_reconnect": replica_applied,
            "replica_reconnect_seconds": reconnect_seconds,
            "replica_applied_before_partition": verified,
            "recovered_after_promotion": recovered,
            "result": "passed",
            "source_primary_logs": primary_logs,
            "promoted_replica_logs": replica.logs(),
        }
    except Exception as error:
        logs = []
        for case in (primary, replica):
            if case and case.container_id:
                logs.append(case.logs())
        raise RuntimeError(f"{error}\nContainer logs:\n" + "\n".join(logs)) from error
    finally:
        if primary:
            primary.close()
        if replica:
            replica.close()
        run(["docker", "network", "rm", network_id], check=False)


def soak(args):
    case = DockerCase(DEFAULT_IMAGE)
    try:
        case.start_server()
        deadline = time.monotonic() + args.soak_seconds
        operations = 0
        samples = []
        counter = 0
        next_sample = time.monotonic()
        with connect(case.port, timeout=10) as stream:
            while time.monotonic() < deadline:
                key = f"soak:key:{counter % 256:03d}".encode()
                if request(stream, b"SET", key, str(counter).encode()) != (
                    b"+",
                    b"OK",
                ):
                    raise AssertionError("soak SET was not acknowledged")
                value = request(stream, b"GET", key)
                if value != (b"$", str(counter).encode()):
                    raise AssertionError(f"soak GET mismatch: {value!r}")
                if request(stream, b"INCR", b"soak:counter") != (b":", counter + 1):
                    raise AssertionError("soak counter did not increment")
                operations += 3
                counter += 1
                if time.monotonic() >= next_sample:
                    info = request(stream, b"INFO")
                    text = info[1].decode(errors="replace") if info[0] == b"$" else ""
                    samples.append(
                        {
                            field: line.split(":", 1)[1]
                            for line in text.splitlines()
                            for field in (
                                "ruvio_persistence_state",
                                "wal_last_sequence",
                                "charged_memory_bytes",
                                "keys",
                            )
                            if line.startswith(field + ":")
                        }
                    )
                    next_sample = time.monotonic() + 1
        with connect(case.port) as stream:
            if request(stream, b"PING") != (b"+", b"PONG"):
                raise AssertionError("final soak health check failed")
        return {
            "duration_seconds": args.soak_seconds,
            "iterations": counter,
            "operations": operations,
            "result": "passed",
            "samples": samples,
        }
    except Exception as error:
        raise RuntimeError(f"{error}\nRuvio logs:\n{case.logs()}") from error
    finally:
        case.close()


SCENARIOS = {
    "disk-full": disk_full,
    "permission-denied": permission_denied,
    "process-crash": process_crash,
    "replica-promotion": replica_promotion,
    "soak": soak,
}


def docker_metadata():
    version = run(["docker", "version", "--format", "{{.Server.Version}}"])
    info = run(["docker", "info", "--format", "{{.OSType}}/{{.Architecture}}"])
    image = run(
        ["docker", "image", "inspect", DEFAULT_IMAGE, "--format", "{{.Id}}"]
    )
    return {
        "docker_engine_version": version.stdout.strip(),
        "docker_server_platform": info.stdout.strip(),
        "image": DEFAULT_IMAGE,
        "image_id": image.stdout.strip(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        choices=[*SCENARIOS, "all"],
        default="all",
        help="fault scenario to run (default: all)",
    )
    parser.add_argument(
        "--max-writes",
        type=int,
        default=20000,
        help="maximum writes per fault case before treating missing failure as a test failure",
    )
    parser.add_argument(
        "--soak-seconds",
        type=float,
        default=30,
        help="duration of the bounded mixed-workload soak scenario (default: 30)",
    )
    args = parser.parse_args()
    if args.max_writes < 1:
        parser.error("--max-writes must be positive")
    if not 0 < args.soak_seconds <= 3600:
        parser.error("--soak-seconds must be greater than 0 and at most 3600")
    if shutil.which("docker") is None:
        parser.error("Docker CLI was not found")

    scenarios = list(SCENARIOS) if args.scenario == "all" else [args.scenario]
    metadata = docker_metadata()
    report = {
        "created_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "boundary": (
            "Container-level process/filesystem fault injection only. "
            "This is not host power-loss, physical-device, or bare-metal evidence."
        ),
        "environment": metadata,
        "scenarios": {},
    }
    failures = 0
    for name in scenarios:
        try:
            report["scenarios"][name] = SCENARIOS[name](args)
            print(f"{name}: {report['scenarios'][name]['result']}")
        except Exception as error:
            failures += 1
            report["scenarios"][name] = {
                "error": str(error),
                "result": "failed",
            }
            print(f"{name}: failed: {error}", file=sys.stderr)

    output_dir = ROOT / "target" / "docker-resilience"
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
