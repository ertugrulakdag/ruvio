# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Synchronous Ruvio client: one socket, a reusable ``recv_into`` buffer, no threads."""

from __future__ import annotations

import socket
import time
import uuid
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union
from urllib.parse import unquote, urlparse

from ._commands import Commands, Conv, KeyT, _s
from ._models import Message
from ._pipeline import PipelineBase
from ._resp import NEED, Push, encode, make_parser
from .errors import ConnectionError, LockError, ResponseError, RuvioError, TimeoutError

__all__ = ["Ruvio", "Pipeline", "PubSub", "Lock", "parse_url"]


def parse_url(url: str) -> Dict[str, Any]:
    """``ruvio://[user:pass@]host[:port][/db]`` (``redis://`` and ``unix://path`` accepted)."""
    u = urlparse(url)
    opts: Dict[str, Any] = {}
    if u.scheme == "unix":
        opts["unix_socket_path"] = u.path
    else:
        if u.scheme not in ("ruvio", "redis", "tcp"):
            raise ValueError(f"unsupported URL scheme {u.scheme!r}")
        opts["host"] = u.hostname or "127.0.0.1"
        opts["port"] = u.port or 6379
        path = u.path.lstrip("/")
        if path:
            opts["db"] = int(path)
    if u.username:
        opts["username"] = unquote(u.username)
    if u.password:
        opts["password"] = unquote(u.password)
    return opts


class _Options:
    __slots__ = (
        "host",
        "port",
        "db",
        "username",
        "password",
        "client_name",
        "socket_timeout",
        "connect_timeout",
        "unix_socket_path",
        "parser",
        "on_push",
    )

    def __init__(self, **kw: Any) -> None:
        for name in self.__slots__:
            setattr(self, name, kw.get(name))

    def handshake(self) -> List[Tuple[Conv, bytes]]:
        cmds: List[Tuple[Conv, bytes]] = []
        if self.password is not None:
            if self.username is not None:
                cmds.append((None, encode("AUTH", (self.username, self.password))))
            else:
                cmds.append((None, encode("AUTH", (self.password,))))
        if self.db:
            cmds.append((None, encode("SELECT", (self.db,))))
        if self.client_name:
            cmds.append((None, encode("CLIENT SETNAME", (self.client_name,))))
        return cmds

    def as_dict(self) -> Dict[str, Any]:
        return {n: getattr(self, n) for n in self.__slots__}


class _Conn:
    """The socket plus its parser. Shared by a client and its ``raw`` view."""

    __slots__ = ("sock", "parser", "opts")

    def __init__(self, opts: _Options) -> None:
        self.opts = opts
        self.sock: Optional[socket.socket] = None
        self.parser: Any = None

    def open(self) -> None:
        o = self.opts
        try:
            if o.unix_socket_path:
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(o.connect_timeout)
                sock.connect(o.unix_socket_path)
            else:
                sock = socket.create_connection((o.host, o.port), timeout=o.connect_timeout)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(o.socket_timeout)
        except socket.timeout as exc:
            raise TimeoutError(f"connect to {o.host}:{o.port} timed out") from exc
        except OSError as exc:
            raise ConnectionError(f"cannot connect to {o.host}:{o.port}: {exc}") from exc
        self.sock = sock
        self.parser = make_parser(o.parser or "auto")
        hs = o.handshake()
        if hs:
            try:
                sock.sendall(b"".join(d for _, d in hs))
                for _ in hs:
                    r = self.read()
                    if isinstance(r, ResponseError):
                        raise r
            except BaseException:
                self.close()
                raise

    def close(self) -> None:
        sock = self.sock
        self.sock = None
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    def read(self) -> Any:
        p = self.parser
        while True:
            v = p.gets()
            if v is not NEED:
                if type(v) is Push:
                    cb = self.opts.on_push
                    if cb is not None:
                        cb(v)
                    continue
                return v
            n = self.sock.recv_into(p.get_buffer())  # type: ignore[union-attr]
            if not n:
                raise ConnectionError("connection closed by server")
            p.end += n


class Ruvio(Commands):
    """Synchronous client. Not thread-safe: use one client per thread (or a lock).

    Commands return converted Python values; server errors raise :class:`ResponseError`.
    A dropped socket is reopened before the next command (the failing command raises).
    ``cluster=True`` routes by key over one socket per node (see :class:`ruvio.cluster.ClusterRuvio`);
    a standalone server is detected on the first command and used as usual.
    """

    __slots__ = ("_conn", "_decode")

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
        socket_timeout: Optional[float] = None,
        connect_timeout: Optional[float] = 5.0,
        decode_responses: bool = True,
        unix_socket_path: Optional[str] = None,
        parser: str = "auto",
        on_push: Optional[Callable[[List[Any]], None]] = None,
        lazy: bool = True,
        cluster: bool = False,
    ) -> None:
        opts = _Options(
            host=host,
            port=port,
            db=db,
            password=password,
            username=username,
            client_name=client_name,
            socket_timeout=socket_timeout,
            connect_timeout=connect_timeout,
            unix_socket_path=unix_socket_path,
            parser=parser,
            on_push=on_push,
        )
        self._conn = _Conn(opts)
        self._decode = decode_responses
        if not lazy:
            self._conn.open()

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> "Ruvio":
        opts = parse_url(url)
        opts.update(kwargs)
        return cls(**opts)

    # ---------------------------------------------------------------- plumbing
    def _x(self, conv: Conv, data: bytes, decode: Optional[bool] = None) -> Any:
        conn = self._conn
        sock = conn.sock
        if sock is None:
            conn.open()
            sock = conn.sock
        p = conn.parser
        dec = self._decode if decode is None else decode
        if p.decode is not dec:
            p.decode = dec
        try:
            sock.sendall(data)  # type: ignore[union-attr]
            reply = conn.read()
        except socket.timeout as exc:
            conn.close()
            raise TimeoutError("timed out waiting for the reply") from exc
        except OSError as exc:
            conn.close()
            if isinstance(exc, ConnectionError):
                raise
            raise ConnectionError(str(exc)) from exc
        except BaseException:
            conn.close()
            raise
        if isinstance(reply, ResponseError):
            raise reply
        if conv is None:
            return reply
        return conv(reply)

    def _batch(self, payload: List[bytes], count: int) -> List[Any]:
        conn = self._conn
        if conn.sock is None:
            conn.open()
        p = conn.parser
        if p.decode is not self._decode:
            p.decode = self._decode
        try:
            conn.sock.sendall(b"".join(payload))  # type: ignore[union-attr]
            return [conn.read() for _ in range(count)]
        except socket.timeout as exc:
            conn.close()
            raise TimeoutError("timed out waiting for pipeline replies") from exc
        except OSError as exc:
            conn.close()
            if isinstance(exc, ConnectionError):
                raise
            raise ConnectionError(str(exc)) from exc
        except BaseException:
            conn.close()
            raise

    def select(self, db: int) -> Any:
        """``SELECT`` - also remembered so a reconnect selects the same database."""
        ok = super().select(db)
        self._conn.opts.db = db
        return ok

    def client_setname(self, name: str) -> Any:
        ok = super().client_setname(name)
        self._conn.opts.client_name = name
        return ok

    def auth(self, password: str, username: Optional[str] = None) -> Any:
        ok = super().auth(password, username)
        self._conn.opts.password = password
        self._conn.opts.username = username
        return ok

    @property
    def raw(self) -> "Ruvio":
        """A view on the same connection whose replies are ``bytes`` (binary-safe)."""
        view = Ruvio.__new__(Ruvio)
        view._conn = self._conn
        view._decode = False
        return view

    def connect(self) -> "Ruvio":
        if self._conn.sock is None:
            self._conn.open()
        return self

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "Ruvio":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def dedicated(self) -> "Ruvio":
        """A new client (own socket) with the same options, e.g. for WATCH or blocking commands."""
        opts = self._conn.opts.as_dict()
        return Ruvio(decode_responses=self._decode, **opts)

    def pipeline(self, transaction: bool = False) -> "Pipeline":
        """Batches commands into one write. ``transaction=True`` wraps them in MULTI/EXEC."""
        return Pipeline(self, transaction)

    def transaction(self, fn: Callable[["Pipeline"], Any], *watch: KeyT, retries: int = 16) -> List[Any]:
        """Optimistic transaction: WATCH keys, call ``fn(pipe)`` (reads go through ``self``),
        EXEC, and retry on :class:`WatchError`."""
        from .errors import WatchError

        for _ in range(retries):
            if watch:
                self.watch(*watch)
            pipe = Pipeline(self, True)
            try:
                fn(pipe)
                return pipe.execute()
            except WatchError:
                continue
            finally:
                if watch:
                    try:
                        self.unwatch()
                    except RuvioError:
                        pass
        raise RuvioError("transaction retries exhausted")

    def pubsub(self, ignore_subscribe_messages: bool = False) -> "PubSub":
        """A Pub/Sub session on its own socket."""
        return PubSub(self._conn.opts, self._decode, ignore_subscribe_messages)

    # ---------------------------------------------------------------- helpers
    def lock(
        self, name: KeyT, ttl_ms: int = 30000, blocking: bool = True, timeout: Optional[float] = None, retry_ms: int = 50
    ) -> "Lock":
        """Distributed lock (``SET NX PX`` / renew ``SET IFEQ PX`` / release ``DELEX IFEQ``)."""
        return Lock(self, name, ttl_ms, blocking, timeout, retry_ms)

    def lease_scope(self, key: KeyT, ttl_ms: int) -> "_Scope":
        """``with client.lease_scope(k, 5000) as fencing_token:`` - LEASE, then RELEASE on exit."""
        return _Scope(self, key, lambda: self.lease(key, ttl_ms), "lease")

    def semaphore_scope(self, key: KeyT, max: int, ttl_ms: int) -> "_Scope":  # noqa: A002
        """``with client.semaphore_scope(k, 3, 5000) as token:`` - SEMAPHORE, then RELEASE on exit."""
        return _Scope(self, key, lambda: self.semaphore(key, max, ttl_ms), "semaphore")

    def get_or_create(
        self,
        key: KeyT,
        factory: Callable[[], Union[str, bytes]],
        ttl_ms: int,
        fill_ms: int = 10000,
        swr_ms: Optional[int] = None,
        timeout: float = 10.0,
        poll_ms: float = 5.0,
    ) -> Any:
        """Cache-aside with stampede protection on ``FILLGET``/``FILLSET``.

        Exactly one caller runs ``factory`` per miss; others poll plain ``FILLGET`` with a short
        backoff (``FILLGET ... WAIT`` would park the connection) until the value appears."""
        deadline = time.monotonic() + timeout
        delay = poll_ms / 1000.0
        while True:
            r = self.fillget(key, fill_ms)
            state = r.state
            if state == "HIT":
                return r.value
            if state == "FILL" or (state == "STALE" and r.token is not None):
                value = factory()
                self.fillset(key, value, ttl_ms, r.token, swr_ms)
                return value
            if state == "STALE":
                return r.value
            if time.monotonic() >= deadline:
                raise TimeoutError(f"get_or_create: {key!r} still being filled after {timeout}s")
            time.sleep(delay)
            delay = min(delay * 2, 0.1)

    def idempotent(
        self, key: KeyT, fingerprint: str, ttl_ms: int, fn: Callable[[], Union[str, bytes]], owner: Optional[str] = None
    ) -> Tuple[str, Any]:
        """Runs ``fn`` at most once per key (``IDEM BEGIN``/``COMPLETE``).

        Returns ``("executed", result)``, ``("completed", stored_result)``, ``("pending", None)``
        while another owner runs it, or ``("conflict", None)`` for a different fingerprint."""
        owner = owner or uuid.uuid4().hex
        r = self.idem_begin(key, fingerprint, owner, ttl_ms)
        if r.state == "acquired":
            result = fn()
            self.idem_complete(key, fingerprint, owner, result)
            return "executed", result
        return r.state, r.result

    def scan_iter(self, match: Optional[str] = None, count: Optional[int] = None, type: Optional[str] = None) -> Iterator[Any]:  # noqa: A002
        cursor = 0
        while True:
            cursor, keys = self.scan(cursor, match, count, type)
            yield from keys
            if cursor == 0:
                return

    def hscan_iter(self, key: KeyT, match: Optional[str] = None, count: Optional[int] = None) -> Iterator[Tuple[Any, Any]]:
        cursor = 0
        while True:
            cursor, page = self.hscan(key, cursor, match, count)
            yield from page.items()
            if cursor == 0:
                return

    def sscan_iter(self, key: KeyT, match: Optional[str] = None, count: Optional[int] = None) -> Iterator[Any]:
        cursor = 0
        while True:
            cursor, page = self.sscan(key, cursor, match, count)
            yield from page
            if cursor == 0:
                return

    def zscan_iter(self, key: KeyT, match: Optional[str] = None, count: Optional[int] = None) -> Iterator[Tuple[Any, float]]:
        cursor = 0
        while True:
            cursor, page = self.zscan(key, cursor, match, count)
            yield from page
            if cursor == 0:
                return


class Pipeline(PipelineBase):
    """``pipe = client.pipeline(); pipe.set(k, v).get(k); results = pipe.execute()``."""

    __slots__ = ()

    def execute(self, raise_on_error: bool = True) -> List[Any]:
        if not self._cmds:
            return []
        payload = self._payload()
        try:
            raw = self._client._batch(payload, len(payload))
            return self._finish(raw, raise_on_error)
        finally:
            self.reset()

    def __enter__(self) -> "Pipeline":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.reset()


class Lock:
    """Distributed lock with a random token. Release never deletes another holder's lock."""

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

    def acquire(self, blocking: Optional[bool] = None, timeout: Optional[float] = None) -> bool:
        blocking = self.blocking if blocking is None else blocking
        timeout = self.timeout if timeout is None else timeout
        token = uuid.uuid4().hex
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if self.client.set(self.name, token, px=self.ttl_ms, nx=True):
                self.token = token
                return True
            if not blocking or (deadline is not None and time.monotonic() >= deadline):
                return False
            time.sleep(self.retry_ms / 1000.0)

    def renew(self, ttl_ms: Optional[int] = None) -> bool:
        """Extends the TTL only while this token still holds the lock."""
        if self.token is None:
            return False
        return bool(self.client.set(self.name, self.token, ifeq=self.token, px=ttl_ms or self.ttl_ms))

    def release(self) -> bool:
        """``DELEX name IFEQ token`` - False when the lock expired or another holder owns it."""
        token, self.token = self.token, None
        if token is None:
            return False
        return bool(self.client.delex(self.name, token))

    def owned(self) -> bool:
        return self.token is not None and self.client.get(self.name) == self.token

    def __enter__(self) -> "Lock":
        if not self.acquire():
            raise LockError(f"could not acquire lock {self.name!r}")
        return self

    def __exit__(self, *exc: Any) -> None:
        self.release()


class _Scope:
    __slots__ = ("client", "key", "acquire", "what", "token")

    def __init__(self, client: Ruvio, key: KeyT, acquire: Callable[[], Any], what: str) -> None:
        self.client = client
        self.key = key
        self.acquire = acquire
        self.what = what
        self.token: Optional[int] = None

    def __enter__(self) -> int:
        token = self.acquire()
        if token is None:
            raise LockError(f"{self.what} {self.key!r} is not available")
        self.token = token
        return token

    def __exit__(self, *exc: Any) -> None:
        if self.token is not None:
            self.client.release(self.key, self.token)
            self.token = None


def _message(v: Any) -> Message:
    if type(v) is not list and type(v) is not Push:
        return Message("reply", None, v)
    kind = _s(v[0])
    if kind == "pmessage":
        return Message(kind, v[2], v[3], v[1])
    if kind == "pong":
        return Message(kind, None, v[1] if len(v) > 1 else None)
    return Message(kind, v[1], v[2] if len(v) > 2 else None)


_SUB_KINDS = frozenset(("subscribe", "unsubscribe", "psubscribe", "punsubscribe", "ssubscribe", "sunsubscribe"))


class PubSub:
    """Pub/Sub on a dedicated socket. Subscriptions are re-sent after a reconnect."""

    def __init__(self, opts: _Options, decode: bool, ignore_subscribe_messages: bool = False) -> None:
        o = _Options(**opts.as_dict())
        o.socket_timeout = None
        self._conn = _Conn(o)
        self._decode = decode
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self.channels: Dict[Any, None] = {}
        self.patterns: Dict[Any, None] = {}
        self.shard_channels: Dict[Any, None] = {}

    def _ensure(self) -> None:
        c = self._conn
        if c.sock is None:
            c.open()
            c.parser.decode = self._decode
            for name, items in (("SUBSCRIBE", self.channels), ("PSUBSCRIBE", self.patterns), ("SSUBSCRIBE", self.shard_channels)):
                if items:
                    c.sock.sendall(encode(name, tuple(items)))  # type: ignore[union-attr]

    def _send(self, name: str, args: Tuple[Any, ...]) -> None:
        self._ensure()
        try:
            self._conn.sock.sendall(encode(name, args))  # type: ignore[union-attr]
        except OSError as exc:
            self._conn.close()
            raise ConnectionError(str(exc)) from exc

    def subscribe(self, *channels: Any) -> None:
        self._send("SUBSCRIBE", channels)
        self.channels.update(dict.fromkeys(channels))

    def unsubscribe(self, *channels: Any) -> None:
        for c in channels or list(self.channels):
            self.channels.pop(c, None)
        self._send("UNSUBSCRIBE", channels)

    def psubscribe(self, *patterns: Any) -> None:
        self._send("PSUBSCRIBE", patterns)
        self.patterns.update(dict.fromkeys(patterns))

    def punsubscribe(self, *patterns: Any) -> None:
        for p in patterns or list(self.patterns):
            self.patterns.pop(p, None)
        self._send("PUNSUBSCRIBE", patterns)

    def ssubscribe(self, *channels: Any) -> None:
        self._send("SSUBSCRIBE", channels)
        self.shard_channels.update(dict.fromkeys(channels))

    def sunsubscribe(self, *channels: Any) -> None:
        for c in channels or list(self.shard_channels):
            self.shard_channels.pop(c, None)
        self._send("SUNSUBSCRIBE", channels)

    def ping(self, message: Optional[str] = None) -> None:
        self._send("PING", () if message is None else (message,))

    def get_message(self, timeout: Optional[float] = 0.0) -> Optional[Message]:
        """Next message, or None after ``timeout`` seconds (None waits forever)."""
        self._ensure()
        c = self._conn
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            p = c.parser
            v = p.gets()
            if v is NEED:
                remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
                c.sock.settimeout(remaining)  # type: ignore[union-attr]
                try:
                    n = c.sock.recv_into(p.get_buffer())  # type: ignore[union-attr]
                except (socket.timeout, BlockingIOError):
                    return None
                except OSError as exc:
                    c.close()
                    raise ConnectionError(str(exc)) from exc
                if not n:
                    c.close()
                    raise ConnectionError("connection closed by server")
                p.end += n
                continue
            if isinstance(v, ResponseError):
                raise v
            m = _message(v)
            if self.ignore_subscribe_messages and m.kind in _SUB_KINDS:
                continue
            return m

    def listen(self) -> Iterator[Message]:
        while True:
            m = self.get_message(timeout=None)
            if m is not None:
                yield m

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "PubSub":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
