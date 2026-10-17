# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Live tests need RUVIO_TEST_ADDR=host:port; without it they are skipped (not failed).

Live tests use logical database 11 (FLUSHDB before each test). JSON.INDEX only works in
database 0, so those tests use a unique key prefix there and clean up after themselves.
"""

from __future__ import annotations

import asyncio
import inspect
import os
import signal
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import ruvio  # noqa: E402
import ruvio.asyncio  # noqa: E402

ADDR = os.environ.get("RUVIO_TEST_ADDR")
DB = 11
PARSER = os.environ.get("RUVIO_TEST_PARSER", "auto")
# Hang guards: a sync live client gives up on a reply after SOCKET_TIMEOUT seconds, and any test
# still running after TEST_TIMEOUT seconds fails (where SIGALRM exists), instead of blocking the
# run until the CI job times out.
SOCKET_TIMEOUT = 10.0
TEST_TIMEOUT = 120


# How long a test waits for something that normally takes milliseconds.
DEADLINE = 10.0


def wait_for(cond, what: str, timeout: float = DEADLINE):
    """Calls ``cond`` until it returns a truthy value and returns that; fails after ``timeout``."""
    deadline = time.monotonic() + timeout
    while True:
        value = cond()
        if value:
            return value
        if time.monotonic() >= deadline:
            pytest.fail(f"timed out after {timeout} s waiting for {what}")
        time.sleep(0.01)


async def await_for(cond, what: str, timeout: float = DEADLINE):
    """Like ``wait_for``; ``cond`` may return an awaitable."""
    deadline = time.monotonic() + timeout
    while True:
        value = cond()
        if inspect.isawaitable(value):
            value = await value
        if value:
            return value
        if time.monotonic() >= deadline:
            pytest.fail(f"timed out after {timeout} s waiting for {what}")
        await asyncio.sleep(0.01)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    if not hasattr(signal, "setitimer"):
        yield
        return

    def expire(signum, frame):
        pytest.fail(f"{item.nodeid} ran longer than {TEST_TIMEOUT} s")

    previous = signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, TEST_TIMEOUT)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def addr():
    if not ADDR:
        pytest.skip("RUVIO_TEST_ADDR not set")
    host, port = ADDR.rsplit(":", 1)
    return host, int(port)


class _Done:
    """Awaitable that is already complete (lets one async scenario drive the sync client)."""

    __slots__ = ("v",)

    def __init__(self, v):
        self.v = v

    def __await__(self):
        return self.v
        yield  # pragma: no cover


class SyncAsAsync:
    """Wraps the sync client so ``await c.get(k)`` calls it synchronously."""

    kind = "sync"

    def __init__(self, client):
        self._c = client

    def __getattr__(self, name):
        attr = getattr(self._c, name)
        if not callable(attr):
            return attr

        def call(*a, **k):
            try:
                return _Done(attr(*a, **k))
            except Exception as exc:  # re-raise at await time, like the async client
                return _Raise(exc)

        return call

    @property
    def raw(self):
        return SyncAsAsync(self._c.raw)

    def dedicated(self):
        return SyncAsAsync(self._c.dedicated())

    async def aclose(self):
        self._c.close()


class _Raise:
    __slots__ = ("e",)

    def __init__(self, e):
        self.e = e

    def __await__(self):
        raise self.e
        yield  # pragma: no cover


@pytest.fixture
def sync_client():
    host, port = addr()
    c = ruvio.Ruvio(host, port, db=DB, parser=PARSER, socket_timeout=SOCKET_TIMEOUT)
    c.flushdb()
    yield c
    c.close()


@pytest.fixture
async def async_client():
    host, port = addr()
    c = ruvio.asyncio.Ruvio(host, port, db=DB, parser=PARSER)
    await c.flushdb()
    yield c
    await c.aclose()


@pytest.fixture(params=["sync", "async"])
async def c(request):
    """The same scenario runs against the sync client and the asyncio client."""
    host, port = addr()
    if request.param == "sync":
        s = ruvio.Ruvio(host, port, db=DB, parser=PARSER, socket_timeout=SOCKET_TIMEOUT)
        s.flushdb()
        w = SyncAsAsync(s)
        yield w
        s.close()
    else:
        a = ruvio.asyncio.Ruvio(host, port, db=DB, parser=PARSER)
        await a.flushdb()
        yield a
        await a.aclose()
