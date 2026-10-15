# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""asyncio Ruvio client: one multiplexed, pipelined connection shared by all coroutines.

* Every command appends ``(future, converter, decode)`` to a FIFO deque and writes its
  pre-encoded frame. Replies arrive in order, so the reader pops the deque.
* The reader is an :class:`asyncio.BufferedProtocol`: the event loop reads straight into
  the parser's reusable buffer (no per-read ``bytes`` objects, no StreamReader).
* Writes: on an idle connection the frame is written immediately (no added latency).
  While replies are in flight, frames queued in the same loop iteration are joined and
  written once (``call_soon`` flush, no timer), capped at 64 KiB per write.
"""

from __future__ import annotations

import asyncio
import inspect
import time
import uuid
from collections import deque
from typing import Any, AsyncIterator, Awaitable, Callable, Deque, Dict, List, Optional, Tuple, Union

from .._commands import Commands, Conv, KeyT
from .._models import Message
from .._pipeline import PipelineBase
from .._resp import NEED, Push, encode, make_parser
from ..client import _SUB_KINDS, _message, _Options, parse_url
from ..errors import ConnectionError, LockError, ResponseError, RuvioError, TimeoutError, WatchError

__all__ = ["Ruvio", "Pipeline", "PubSub", "Lock"]

_WRITE_CAP = 65536


class _Batch:
    """Collects ``count`` consecutive replies into one future (pipelines)."""

    __slots__ = ("fut", "left", "items")

    def __init__(self, fut: "asyncio.Future[Any]", count: int) -> None:
        self.fut = fut
        self.left = count
        self.items: List[Any] = []

    def add(self, v: Any) -> None:
        self.items.append(v)
        self.left -= 1
        if not self.left and not self.fut.done():
            self.fut.set_result(self.items)


class _Proto(asyncio.BufferedProtocol):
    def __init__(self, parser: Any, loop: asyncio.AbstractEventLoop, on_push: Optional[Callable[[Any], None]]) -> None:
        self.parser = parser
        self.loop = loop
        self.pending: Deque[Tuple[Any, Conv, bool]] = deque()
        self.transport: Optional[asyncio.Transport] = None
        self.wbuf: Optional[List[bytes]] = None
        self.wsize = 0
        self.on_push = on_push
        self.closed = False
        self.on_lost: Optional[Callable[["_Proto"], None]] = None
        self.lost_exc: Optional[BaseException] = None

    # -- transport callbacks -------------------------------------------------
    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def get_buffer(self, sizehint: int) -> memoryview:
        return self.parser.get_buffer(16384)

    def buffer_updated(self, nbytes: int) -> None:
        p = self.parser
        p.end += nbytes
        pending = self.pending
        while True:
            if pending:
                dec = pending[0][2]
                if p.decode is not dec:
                    p.decode = dec
            try:
                v = p.gets()
            except Exception as exc:  # protocol error: the stream is unusable
                self._fail(exc)
                if self.transport is not None:
                    self.transport.close()
                return
            if v is NEED:
                return
            if not pending or type(v) is Push:
                if self.on_push is not None:
                    self.on_push(v)
                continue
            target, conv, _ = pending.popleft()
            if target.__class__ is _Batch:
                target.add(v)
                continue
            if target.done():  # cancelled by the caller; the reply is dropped
                continue
            if isinstance(v, ResponseError):
                target.set_exception(v)
            elif conv is None:
                target.set_result(v)
            else:
                try:
                    target.set_result(conv(v))
                except Exception as exc:  # noqa: BLE001
                    target.set_exception(exc)

    def eof_received(self) -> Optional[bool]:
        return False

    def connection_lost(self, exc: Optional[BaseException]) -> None:
        self.closed = True
        self.wbuf = None
        self._fail(ConnectionError(f"connection lost: {exc}" if exc else "connection closed by server"))
        if self.on_lost is not None:
            self.on_lost(self)

    def _fail(self, exc: BaseException) -> None:
        self.lost_exc = exc
        pending = self.pending
        while pending:
            target = pending.popleft()[0]
            fut = target.fut if target.__class__ is _Batch else target
            if not fut.done():
                fut.set_exception(exc)

    # -- writing -------------------------------------------------------------
    def send(self, data: bytes) -> None:
        wbuf = self.wbuf
        if wbuf is not None:
            wbuf.append(data)
            self.wsize += len(data)
            if self.wsize >= _WRITE_CAP:
                self.flush()
        elif len(self.pending) <= 1:
            self.transport.write(data)  # type: ignore[union-attr]
        else:
            self.wbuf = [data]
            self.wsize = len(data)
            self.loop.call_soon(self.flush)

    def flush(self) -> None:
        wbuf = self.wbuf
        if wbuf is None:
            return
        self.wbuf = None
        if self.closed:
            return
        self.transport.write(wbuf[0] if len(wbuf) == 1 else b"".join(wbuf))  # type: ignore[union-attr]


class _State:
    """Connection state shared by a client and its ``raw`` view."""

    __slots__ = ("opts", "proto", "lock", "loop")

    def __init__(self, opts: _Options) -> None:
        self.opts = opts
        self.proto: Optional[_Proto] = None
        self.lock: Optional[asyncio.Lock] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None


async def _open(opts: _Options, on_push: Optional[Callable[[Any], None]] = None) -> _Proto:
    loop = asyncio.get_running_loop()
    parser = make_parser(opts.parser or "auto")
    try:
        if opts.unix_socket_path:
            coro = loop.create_unix_connection(lambda: _Proto(parser, loop, on_push), opts.unix_socket_path)
        else:
            coro = loop.create_connection(lambda: _Proto(parser, loop, on_push), opts.host, opts.port)
        _, proto = await asyncio.wait_for(coro, opts.connect_timeout)
    except asyncio.TimeoutError as exc:
        raise TimeoutError(f"connect to {opts.host}:{opts.port} timed out") from exc
    except OSError as exc:
        raise ConnectionError(f"cannot connect to {opts.host}:{opts.port}: {exc}") from exc
    hs = opts.handshake()
    if hs:
        futs = []
        for conv, data in hs:
            fut = loop.create_future()
            proto.pending.append((fut, conv, True))
            futs.append(fut)
        proto.transport.write(b"".join(d for _, d in hs))  # type: ignore[union-attr]
        try:
            for fut in futs:
                await fut
        except BaseException:
            proto.transport.close()  # type: ignore[union-attr]
            raise
    return proto


class Ruvio(Commands):
    """asyncio client. ``await client.get("k")``; safe to share between coroutines.

    Blocking commands (BLPOP, XREAD BLOCK, FILLGET WAIT ...) hold the shared connection;
    run them on :meth:`dedicated`. A lost connection fails the in-flight commands and is
    reopened by the next command. ``cluster=True`` routes by key over one connection per node
    (:class:`ruvio.asyncio.cluster.ClusterRuvio`); a standalone server is detected on the first command.
    """

    __slots__ = ("_st", "_decode")

    def __new__(cls, *args: Any, cluster: bool = False, **kwargs: Any) -> "Ruvio":
        # ``Ruvio(..., cluster=True)`` builds the cluster-routing subclass; the plain class is untouched.
        if cluster and cls is Ruvio:
            from .cluster import ClusterRuvio

            cls = ClusterRuvio  # type: ignore[assignment]
        return object.__new__(cls)

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        username: Optional[str] = None,
        client_name: Optional[str] = None,
        connect_timeout: Optional[float] = 5.0,
        decode_responses: bool = True,
        unix_socket_path: Optional[str] = None,
        parser: str = "auto",
        on_push: Optional[Callable[[List[Any]], None]] = None,
        cluster: bool = False,
    ) -> None:
        self._st = _State(
            _Options(
                host=host,
                port=port,
                db=db,
                password=password,
                username=username,
                client_name=client_name,
                connect_timeout=connect_timeout,
                unix_socket_path=unix_socket_path,
                parser=parser,
                on_push=on_push,
            )
        )
        self._decode = decode_responses

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> "Ruvio":
        opts = parse_url(url)
        opts.update(kwargs)
        return cls(**opts)

    # ---------------------------------------------------------------- plumbing
    def _x(self, conv: Conv, data: bytes, decode: Optional[bool] = None) -> Any:
        proto = self._st.proto
        if proto is None:
            return self._connect_then(conv, data, decode)
        fut = proto.loop.create_future()
        proto.pending.append((fut, conv, self._decode if decode is None else decode))
        proto.send(data)
        return fut

    async def _connect_then(self, conv: Conv, data: bytes, decode: Optional[bool]) -> Any:
        await self.connect()
        return await self._x(conv, data, decode)

    async def connect(self) -> "Ruvio":
        st = self._st
        if st.proto is not None:
            return self
        if st.lock is None:
            st.lock = asyncio.Lock()
        async with st.lock:
            if st.proto is None:
                proto = await _open(st.opts, st.opts.on_push)

                def lost(p: _Proto, st: _State = st) -> None:
                    if st.proto is p:
                        st.proto = None

                proto.on_lost = lost
                st.proto = proto
        return self

    def _batch(self, payload: List[bytes]) -> "Awaitable[List[Any]]":
        proto = self._st.proto
        if proto is None:
            return self._connect_batch(payload)
        fut = proto.loop.create_future()
        idle = not proto.pending and proto.wbuf is None
        # one deque slot per expected reply, all feeding the same collector
        entry = (_Batch(fut, len(payload)), None, self._decode)
        proto.pending.extend([entry] * len(payload))
        data = b"".join(payload)
        if idle:
            proto.transport.write(data)  # type: ignore[union-attr]
        else:
            proto.send(data)
        return fut

    async def _connect_batch(self, payload: List[bytes]) -> List[Any]:
        await self.connect()
        return await self._batch(payload)

    async def select(self, db: int) -> Any:  # type: ignore[override]
        """``SELECT`` for the whole shared connection; remembered for reconnects."""
        ok = await super().select(db)
        self._st.opts.db = db
        return ok

    async def client_setname(self, name: str) -> Any:  # type: ignore[override]
        ok = await super().client_setname(name)
        self._st.opts.client_name = name
        return ok

    async def auth(self, password: str, username: Optional[str] = None) -> Any:  # type: ignore[override]
        ok = await super().auth(password, username)
        self._st.opts.password = password
        self._st.opts.username = username
        return ok

    @property
    def raw(self) -> "Ruvio":
        """A view on the same connection whose replies are ``bytes``."""
        view = Ruvio.__new__(Ruvio)
        view._st = self._st
        view._decode = False
        return view

    async def aclose(self) -> None:
        proto = self._st.proto
        self._st.proto = None
        if proto is not None and proto.transport is not None:
            proto.transport.close()
            await asyncio.sleep(0)

    close = aclose

    async def __aenter__(self) -> "Ruvio":
        return await self.connect()

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    def dedicated(self) -> "Ruvio":
        """A new client (own connection) with the same options - for WATCH and blocking commands."""
        return Ruvio(decode_responses=self._decode, **{k: v for k, v in self._st.opts.as_dict().items() if k != "socket_timeout"})

    def pipeline(self, transaction: bool = False) -> "Pipeline":
        return Pipeline(self, transaction)

    async def pubsub(self, ignore_subscribe_messages: bool = False) -> "PubSub":
        ps = PubSub(self._st.opts, self._decode, ignore_subscribe_messages)
        await ps.connect()
        return ps

    # ---------------------------------------------------------------- helpers
    def lock(
        self, name: KeyT, ttl_ms: int = 30000, blocking: bool = True, timeout: Optional[float] = None, retry_ms: int = 50
    ) -> "Lock":
        """``async with client.lock("job", 10000) as lk:`` - SET NX PX / SET IFEQ PX / DELEX IFEQ."""
        return Lock(self, name, ttl_ms, blocking, timeout, retry_ms)

    def lease_scope(self, key: KeyT, ttl_ms: int) -> "_Scope":
        return _Scope(self, key, lambda: self.lease(key, ttl_ms), "lease")

    def semaphore_scope(self, key: KeyT, max: int, ttl_ms: int) -> "_Scope":  # noqa: A002
        return _Scope(self, key, lambda: self.semaphore(key, max, ttl_ms), "semaphore")

    async def get_or_create(
        self,
        key: KeyT,
        factory: Callable[[], Union[str, bytes, Awaitable[Union[str, bytes]]]],
        ttl_ms: int,
        fill_ms: int = 10000,
        swr_ms: Optional[int] = None,
        timeout: float = 10.0,
        poll_ms: float = 5.0,
    ) -> Any:
        """Cache-aside on FILLGET/FILLSET; ``factory`` may be sync or async. Waiters poll plain
        FILLGET with backoff (``WAIT`` would park the shared connection)."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        delay = poll_ms / 1000.0
        while True:
            r = await self.fillget(key, fill_ms)
            state = r.state
            if state == "HIT":
                return r.value
            if state == "FILL" or (state == "STALE" and r.token is not None):
                value = factory()
                if inspect.isawaitable(value):
                    value = await value
                await self.fillset(key, value, ttl_ms, r.token, swr_ms)
                return value
            if state == "STALE":
                return r.value
            if loop.time() >= deadline:
                raise TimeoutError(f"get_or_create: {key!r} still being filled after {timeout}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 0.1)

    async def idempotent(
        self,
        key: KeyT,
        fingerprint: str,
        ttl_ms: int,
        fn: Callable[[], Union[str, bytes, Awaitable[Union[str, bytes]]]],
        owner: Optional[str] = None,
    ) -> Tuple[str, Any]:
        """See :meth:`ruvio.Ruvio.idempotent`."""
        owner = owner or uuid.uuid4().hex
        r = await self.idem_begin(key, fingerprint, owner, ttl_ms)
        if r.state == "acquired":
            result = fn()
            if inspect.isawaitable(result):
                result = await result
            await self.idem_complete(key, fingerprint, owner, result)
            return "executed", result
        return r.state, r.result

    async def transaction(self, fn: Callable[["Pipeline"], Any], *watch: KeyT, retries: int = 16) -> List[Any]:
        """WATCH + MULTI/EXEC with retries, on a dedicated connection (WATCH is per connection)."""
        conn = self.dedicated()
        try:
            for _ in range(retries):
                if watch:
                    await conn.watch(*watch)
                pipe = Pipeline(conn, True)
                try:
                    r = fn(pipe)
                    if inspect.isawaitable(r):
                        await r
                    return await pipe.execute()
                except WatchError:
                    continue
            raise RuvioError("transaction retries exhausted")
        finally:
            await conn.aclose()

    async def scan_iter(
        self, match: Optional[str] = None, count: Optional[int] = None, type: Optional[str] = None  # noqa: A002
    ) -> AsyncIterator[Any]:
        cursor = 0
        while True:
            cursor, keys = await self.scan(cursor, match, count, type)
            for k in keys:
                yield k
            if cursor == 0:
                return

    async def hscan_iter(self, key: KeyT, match: Optional[str] = None, count: Optional[int] = None) -> AsyncIterator[Tuple[Any, Any]]:
        cursor = 0
        while True:
            cursor, page = await self.hscan(key, cursor, match, count)
            for item in page.items():
                yield item
            if cursor == 0:
                return

    async def sscan_iter(self, key: KeyT, match: Optional[str] = None, count: Optional[int] = None) -> AsyncIterator[Any]:
        cursor = 0
        while True:
            cursor, page = await self.sscan(key, cursor, match, count)
            for m in page:
                yield m
            if cursor == 0:
                return

    async def zscan_iter(self, key: KeyT, match: Optional[str] = None, count: Optional[int] = None) -> AsyncIterator[Tuple[Any, float]]:
        cursor = 0
        while True:
            cursor, page = await self.zscan(key, cursor, match, count)
            for m in page:
                yield m
            if cursor == 0:
                return


class Pipeline(PipelineBase):
    """``pipe = client.pipeline(); pipe.set(k, v).get(k); results = await pipe.execute()``."""

    __slots__ = ()

    async def execute(self, raise_on_error: bool = True) -> List[Any]:
        if not self._cmds:
            return []
        payload = self._payload()
        try:
            raw = await self._client._batch(payload)
            return self._finish(raw, raise_on_error)
        finally:
            self.reset()

    async def __aenter__(self) -> "Pipeline":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        self.reset()


class Lock:
    """Async distributed lock; release never deletes another holder's lock."""

    __slots__ = ("client", "name", "ttl_ms", "blocking", "timeout", "retry_ms", "token")

    def __init__(
        self, client: Ruvio, name: KeyT, ttl_ms: int, blocking: bool = True, timeout: Optional[float] = None, retry_ms: int = 50
    ) -> None:
        self.client = client
        self.name = name
        self.ttl_ms = ttl_ms
        self.blocking = blocking
        self.timeout = timeout
        self.retry_ms = retry_ms
        self.token: Optional[str] = None

    async def acquire(self, blocking: Optional[bool] = None, timeout: Optional[float] = None) -> bool:
        blocking = self.blocking if blocking is None else blocking
        timeout = self.timeout if timeout is None else timeout
        token = uuid.uuid4().hex
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if await self.client.set(self.name, token, px=self.ttl_ms, nx=True):
                self.token = token
                return True
            if not blocking or (deadline is not None and time.monotonic() >= deadline):
                return False
            await asyncio.sleep(self.retry_ms / 1000.0)

    async def renew(self, ttl_ms: Optional[int] = None) -> bool:
        if self.token is None:
            return False
        return bool(await self.client.set(self.name, self.token, ifeq=self.token, px=ttl_ms or self.ttl_ms))

    async def release(self) -> bool:
        token, self.token = self.token, None
        if token is None:
            return False
        return bool(await self.client.delex(self.name, token))

    async def owned(self) -> bool:
        return self.token is not None and (await self.client.get(self.name)) == self.token

    async def __aenter__(self) -> "Lock":
        if not await self.acquire():
            raise LockError(f"could not acquire lock {self.name!r}")
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.release()


class _Scope:
    __slots__ = ("client", "key", "acquire", "what", "token")

    def __init__(self, client: Ruvio, key: KeyT, acquire: Callable[[], Any], what: str) -> None:
        self.client = client
        self.key = key
        self.acquire = acquire
        self.what = what
        self.token: Optional[int] = None

    async def __aenter__(self) -> int:
        token = await self.acquire()
        if token is None:
            raise LockError(f"{self.what} {self.key!r} is not available")
        self.token = token
        return token

    async def __aexit__(self, *exc: Any) -> None:
        if self.token is not None:
            await self.client.release(self.key, self.token)
            self.token = None


class PubSub:
    """Pub/Sub on its own connection. ``async for msg in ps.listen(): ...``"""

    def __init__(self, opts: _Options, decode: bool, ignore_subscribe_messages: bool = False) -> None:
        self._opts = opts
        self._decode = decode
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self._queue: "asyncio.Queue[Any]" = asyncio.Queue()
        self._proto: Optional[_Proto] = None
        self.channels: Dict[Any, None] = {}
        self.patterns: Dict[Any, None] = {}
        self.shard_channels: Dict[Any, None] = {}

    async def connect(self) -> None:
        if self._proto is not None and not self._proto.closed:
            return
        proto = await _open(self._opts, self._queue.put_nowait)
        proto.parser.decode = self._decode
        proto.on_lost = lambda p: self._queue.put_nowait(p.lost_exc or ConnectionError("pubsub connection lost"))
        self._proto = proto
        for name, items in (("SUBSCRIBE", self.channels), ("PSUBSCRIBE", self.patterns), ("SSUBSCRIBE", self.shard_channels)):
            if items:
                proto.transport.write(encode(name, tuple(items)))  # type: ignore[union-attr]

    async def _send(self, name: str, args: Tuple[Any, ...]) -> None:
        if self._proto is None or self._proto.closed:
            await self.connect()
        self._proto.transport.write(encode(name, args))  # type: ignore[union-attr]

    async def subscribe(self, *channels: Any) -> None:
        await self._send("SUBSCRIBE", channels)
        self.channels.update(dict.fromkeys(channels))

    async def unsubscribe(self, *channels: Any) -> None:
        for c in channels or list(self.channels):
            self.channels.pop(c, None)
        await self._send("UNSUBSCRIBE", channels)

    async def psubscribe(self, *patterns: Any) -> None:
        await self._send("PSUBSCRIBE", patterns)
        self.patterns.update(dict.fromkeys(patterns))

    async def punsubscribe(self, *patterns: Any) -> None:
        for p in patterns or list(self.patterns):
            self.patterns.pop(p, None)
        await self._send("PUNSUBSCRIBE", patterns)

    async def ssubscribe(self, *channels: Any) -> None:
        await self._send("SSUBSCRIBE", channels)
        self.shard_channels.update(dict.fromkeys(channels))

    async def sunsubscribe(self, *channels: Any) -> None:
        for c in channels or list(self.shard_channels):
            self.shard_channels.pop(c, None)
        await self._send("SUNSUBSCRIBE", channels)

    async def ping(self, message: Optional[str] = None) -> None:
        await self._send("PING", () if message is None else (message,))

    async def get_message(self, timeout: Optional[float] = None) -> Optional[Message]:
        """Next message, or None after ``timeout`` seconds (None waits forever)."""
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            remaining = None if deadline is None else max(0.0, deadline - loop.time())
            try:
                v = await asyncio.wait_for(self._queue.get(), remaining)
            except asyncio.TimeoutError:
                return None
            if isinstance(v, BaseException):
                self._proto = None
                raise v
            m = _message(v)
            if self.ignore_subscribe_messages and m.kind in _SUB_KINDS:
                continue
            return m

    async def listen(self) -> AsyncIterator[Message]:
        while True:
            m = await self.get_message()
            if m is not None:
                yield m

    async def aclose(self) -> None:
        proto, self._proto = self._proto, None
        if proto is not None and proto.transport is not None:
            proto.on_lost = None
            proto.transport.close()

    close = aclose

    async def __aenter__(self) -> "PubSub":
        await self.connect()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

