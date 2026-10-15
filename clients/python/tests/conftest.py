# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Live tests need RUVIO_TEST_ADDR=host:port; without it they are skipped (not failed).

Live tests use logical database 11 (FLUSHDB before each test). JSON.INDEX only works in
database 0, so those tests use a unique key prefix there and clean up after themselves.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import ruvio  # noqa: E402
import ruvio.asyncio  # noqa: E402

ADDR = os.environ.get("RUVIO_TEST_ADDR")
DB = 11
PARSER = os.environ.get("RUVIO_TEST_PARSER", "auto")


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
    c = ruvio.Ruvio(host, port, db=DB, parser=PARSER)
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
        s = ruvio.Ruvio(host, port, db=DB, parser=PARSER)
        s.flushdb()
        w = SyncAsAsync(s)
        yield w
        s.close()
    else:
        a = ruvio.asyncio.Ruvio(host, port, db=DB, parser=PARSER)
        await a.flushdb()
        yield a
        await a.aclose()
