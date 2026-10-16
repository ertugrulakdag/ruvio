"""Helpers shared by the scripts in this folder: RESP framing, a free port, waiting for a server.

New scripts import from here instead of writing their own; older ones move over when they are
next changed. Run a script from anywhere: Python puts the script's own folder on `sys.path`.
"""

from __future__ import annotations

import socket
import time


def encode(*arguments: bytes) -> bytes:
    """One RESP2 command frame."""
    frame = [f"*{len(arguments)}\r\n".encode()]
    for argument in arguments:
        frame.extend((f"${len(argument)}\r\n".encode(), argument, b"\r\n"))
    return b"".join(frame)


def read_response(stream):
    """Reads one reply from a buffered binary stream as `(prefix, value)`; arrays nest."""
    prefix = stream.read(1)
    line = stream.readline()
    if not prefix or not line.endswith(b"\r\n"):
        raise AssertionError("truncated RESP response")
    payload = line[:-2]
    if prefix in (b"+", b"-"):
        return prefix, payload
    if prefix == b":":
        return prefix, int(payload)
    if prefix == b"$":
        length = int(payload)
        if length == -1:
            return prefix, None
        value = stream.read(length)
        assert stream.read(2) == b"\r\n"
        return prefix, value
    if prefix == b"*":
        count = int(payload)
        if count < 0:
            return prefix, None
        return prefix, [read_response(stream) for _ in range(count)]
    raise AssertionError(f"unsupported response prefix: {prefix!r}")


def request(stream, *arguments: bytes):
    """Sends one command and reads its reply."""
    stream.write(encode(*arguments))
    stream.flush()
    return read_response(stream)


def free_port() -> int:
    """A port that was free a moment ago. Another process can take it before the caller binds
    it, so a server started on it should be checked (see `wait_for_port`)."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def wait_for_port(host: str, port: int, timeout: float = 20.0, process=None) -> None:
    """Waits until `host:port` accepts a connection. With `process` (a `subprocess.Popen`), fails
    as soon as that process exits."""
    deadline = time.monotonic() + timeout
    while True:
        if process is not None and process.poll() is not None:
            raise RuntimeError(f"process exited with {process.returncode} before {host}:{port} opened")
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"{host}:{port} did not open within {timeout} s")
            time.sleep(0.02)
