# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Typed command methods shared by the sync client, the asyncio client and pipelines.

Every method encodes its command and hands ``(converter, bytes)`` to ``self._x``.
The sync client sends and returns the converted reply, the asyncio client returns
an awaitable future, and a pipeline queues the command and returns itself. The
converter runs on the raw reply exactly once (in the reader for asyncio).
"""

from __future__ import annotations

import json as _json
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

from ._resp import _BULK_LEN, _CRLF, _TABLE, encode, head

__all__ = ["Commands", "Arg"]

Arg = Union[str, bytes, int, float]

from ._converters import (  # noqa: F401
    Conv,
    KeyT,
    _bf_info,
    _bloom_seen,
    _bool,
    _bools,
    _bzpop,
    _changes,
    _entries,
    _expire_cond,
    _fill,
    _flat_pairs,
    _float,
    _floats,
    _geo_rows,
    _geopos,
    _hello,
    _idem,
    _info,
    _int_or_none,
    _json_load,
    _key_value,
    _keys,
    _limit,
    _limit_args,
    _list_of_dicts,
    _mpop,
    _number,
    _ok,
    _pairs,
    _pairs_text_keys,
    _s,
    _scan,
    _scan_pairs,
    _scan_scored,
    _scored,
    _set,
    _set_reply,
    _text,
    _texts,
    _xautoclaim,
    _xclaim,
    _xinfo_stream,
    _xpending,
    _xpending_range,
    _xread,
)

_GET = b"*2\r\n$3\r\nGET\r\n"
_SET = b"*3\r\n$3\r\nSET\r\n"
_DEL = head("DEL")[1]
_INCR = b"*2\r\n$4\r\nINCR\r\n"
_HGET = b"*3\r\n$4\r\nHGET\r\n"


# --------------------------------------------------------------------------- commands


class Commands:
    """Typed Ruvio commands. Sync clients return values; asyncio clients return awaitables."""

    __slots__ = ()

    def _x(self, conv: Conv, data: bytes, decode: Optional[bool] = None) -> Any:  # pragma: no cover
        raise NotImplementedError

    def _c(self, conv: Conv, name: str, *args: Any) -> Any:
        return self._x(conv, encode(name, args))

    def execute_command(self, *args: Arg) -> Any:
        """Sends any command, e.g. ``execute_command("SET", "k", "v", "PX", 100)``. Returns the raw reply."""
        name = args[0]
        if type(name) is not str:
            name = _s(bytes(name) if not isinstance(name, bytes) else name)
        return self._x(None, encode(name, args[1:]))

    # ======================================================= connection
    def ruvio(self, option: Optional[str] = None) -> Any:
        """``RUVIO [OWNER]`` - identity check; ``OWNER`` names the shard rule."""
        return self._c(None, "RUVIO") if option is None else self._c(None, "RUVIO", option)

    def ping(self, message: Optional[Arg] = None) -> Any:
        return self._c(None, "PING") if message is None else self._c(None, "PING", message)

    def echo(self, message: Arg) -> Any:
        return self._c(None, "ECHO", message)

    def hello(
        self,
        protover: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        setname: Optional[str] = None,
    ) -> Any:
        """``HELLO [2|3] [AUTH user pass] [SETNAME name]``. Note: HELLO 3 switches the
        connection to RESP3 (typed converters accept both shapes)."""
        args: List[Any] = []
        if protover is not None:
            args.append(protover)
            if password is not None:
                args += ["AUTH", username or "default", password]
            if setname is not None:
                args += ["SETNAME", setname]
        return self._c(_hello, "HELLO", *args)

    def auth(self, password: str, username: Optional[str] = None) -> Any:
        if username is None:
            return self._c(_ok, "AUTH", password)
        return self._c(_ok, "AUTH", username, password)

    def select(self, db: int) -> Any:
        return self._c(_ok, "SELECT", db)

    def command(self) -> Any:
        return self._c(None, "COMMAND")

    def command_count(self) -> Any:
        return self._c(None, "COMMAND COUNT")

    def command_info(self, *names: str) -> Any:
        return self._c(None, "COMMAND INFO", *names)

    def command_docs(self, *names: str) -> Any:
        return self._c(None, "COMMAND DOCS", *names)

    def client_getname(self) -> Any:
        return self._c(None, "CLIENT GETNAME")

    def client_setname(self, name: str) -> Any:
        return self._c(_ok, "CLIENT SETNAME", name)

    def client_tracking(self, on: bool) -> Any:
        """``CLIENT TRACKING ON|OFF`` (ON requires RESP3, see :meth:`hello`)."""
        return self._c(_ok, "CLIENT TRACKING", "ON" if on else "OFF")

    def client_id(self) -> Any:
        return self._c(None, "CLIENT ID")

    def client_list(self, type: Optional[str] = None) -> Any:  # noqa: A002
        return self._c(_info, "CLIENT LIST") if type is None else self._c(_info, "CLIENT LIST", "TYPE", type)

    def client_info(self) -> Any:
        return self._c(_info, "CLIENT INFO")

    def client_kill(
        self, addr: Optional[str] = None, *, id: Optional[int] = None, skipme: Optional[bool] = None, legacy: bool = False
    ) -> Any:  # noqa: A002
        """``CLIENT KILL`` by ``ID``/``ADDR`` filters (count reply) or the legacy ``addr`` form."""
        if legacy:
            return self._c(_ok, "CLIENT KILL", addr)
        args: List[Any] = []
        if id is not None:
            args += ["ID", id]
        if addr is not None:
            args += ["ADDR", addr]
        if skipme is not None:
            args += ["SKIPME", "yes" if skipme else "no"]
        return self._c(None, "CLIENT KILL", *args)

    def slowlog_get(self, count: Optional[int] = None) -> Any:
        return self._c(None, "SLOWLOG GET") if count is None else self._c(None, "SLOWLOG GET", count)

    def slowlog_len(self) -> Any:
        return self._c(None, "SLOWLOG LEN")

    def slowlog_reset(self) -> Any:
        return self._c(_ok, "SLOWLOG RESET")

    def latency_latest(self) -> Any:
        return self._c(None, "LATENCY LATEST")

    # ======================================================= acl
    def acl_whoami(self) -> Any:
        return self._c(_text, "ACL WHOAMI")

    def acl_users(self) -> Any:
        return self._c(_texts, "ACL USERS")

    def acl_list(self) -> Any:
        return self._c(_texts, "ACL LIST")

    def acl_getuser(self, username: str) -> Any:
        return self._c(lambda r: None if r is None else _pairs_text_keys(r), "ACL GETUSER", username)

    def acl_cat(self, category: Optional[str] = None) -> Any:
        return self._c(_texts, "ACL CAT") if category is None else self._c(_texts, "ACL CAT", category)

    def acl_setuser(self, username: str, *rules: str) -> Any:
        """``ACL SETUSER name rule...`` e.g. ``acl_setuser("app", "on", ">pw", "+get", "~app:*")``."""
        return self._c(_ok, "ACL SETUSER", username, *rules)

    def acl_load(self) -> Any:
        return self._c(_ok, "ACL LOAD")

    def acl_save(self) -> Any:
        return self._c(_ok, "ACL SAVE")

    # ======================================================= strings
    def get(self, key: KeyT) -> Any:
        """``GET`` - the value or None."""
        if type(key) is str:
            key = key.encode("utf-8", "surrogateescape")
        n = len(key)
        return self._x(None, b"".join((_GET, _BULK_LEN[n] if n < _TABLE else b"$%d\r\n" % n, key, _CRLF)))

    def set(
        self,
        key: KeyT,
        value: Arg,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        exat: Optional[int] = None,
        pxat: Optional[int] = None,
        keepttl: bool = False,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
        ifeq: Optional[Arg] = None,
    ) -> Any:
        """``SET``. Returns True when written, False when NX/XX/IFEQ refused it, or the
        previous value with ``get=True``. ``ifeq`` writes only when the current value equals it."""
        if ex is None and px is None and exat is None and pxat is None and not (keepttl or nx or xx or get) and ifeq is None:
            t = type(key)
            if t is str:
                key = key.encode("utf-8", "surrogateescape")  # type: ignore[union-attr]
            elif t is not bytes:
                return self._c(_set_reply, "SET", key, value)
            t = type(value)
            if t is str:
                value = value.encode("utf-8", "surrogateescape")  # type: ignore[union-attr]
            elif t is not bytes:
                return self._c(_set_reply, "SET", key, value)
            n = len(key)
            m = len(value)  # type: ignore[arg-type]
            return self._x(
                _set_reply,
                b"".join(
                    (
                        _SET,
                        _BULK_LEN[n] if n < _TABLE else b"$%d\r\n" % n,
                        key,  # type: ignore[arg-type]
                        _CRLF,
                        _BULK_LEN[m] if m < _TABLE else b"$%d\r\n" % m,
                        value,  # type: ignore[arg-type]
                        _CRLF,
                    )
                ),
            )
        args: List[Any] = [key, value]
        if ifeq is not None:
            args += ["IFEQ", ifeq]
        if ex is not None:
            args += ["EX", ex]
        if px is not None:
            args += ["PX", px]
        if exat is not None:
            args += ["EXAT", exat]
        if pxat is not None:
            args += ["PXAT", pxat]
        if keepttl:
            args.append("KEEPTTL")
        if nx:
            args.append("NX")
        if xx:
            args.append("XX")
        if get:
            args.append("GET")
            return self._c(None, "SET", *args)
        return self._c(_set_reply, "SET", *args)

    def setv(self, key: KeyT, value: Arg, version: Optional[int] = None) -> Any:
        """``SETV`` versioned write; returns the new version (omit ``version`` to create v1)."""
        if version is None:
            return self._c(None, "SETV", key, value)
        return self._c(None, "SETV", key, value, version)

    def take(self, key: KeyT, count: int) -> Any:
        """``TAKE`` - subtracts ``count`` when available; remainder or None (unchanged)."""
        return self._c(None, "TAKE", key, count)

    def delex(self, key: KeyT, ifeq: Arg) -> Any:
        """``DELEX key IFEQ value`` - deletes only when the value matches. True when deleted."""
        return self._c(_bool, "DELEX", key, "IFEQ", ifeq)

    def getex(self, key: KeyT, ttl_ms: int) -> Any:
        """``GETEX key ms`` - returns the value and sets its TTL to ``ttl_ms`` milliseconds."""
        return self._c(None, "GETEX", key, ttl_ms)

    def mget(self, *keys: KeyT) -> Any:
        if len(keys) == 1 and not isinstance(keys[0], (str, bytes)):
            keys = tuple(keys[0])  # type: ignore[arg-type]
        return self._c(None, "MGET", *keys)

    def mset(self, mapping: Union[Mapping[Any, Any], Iterable[Tuple[Any, Any]]]) -> Any:
        return self._c(_ok, "MSET", *_flat_pairs(mapping))

    def msetnx(self, mapping: Union[Mapping[Any, Any], Iterable[Tuple[Any, Any]]]) -> Any:
        return self._c(_bool, "MSETNX", *_flat_pairs(mapping))

    def getset(self, key: KeyT, value: Arg) -> Any:
        return self._c(None, "GETSET", key, value)

    def append(self, key: KeyT, value: Arg) -> Any:
        return self._c(None, "APPEND", key, value)

    def strlen(self, key: KeyT) -> Any:
        return self._c(None, "STRLEN", key)

    def incr(self, key: KeyT) -> Any:
        if type(key) is str:
            key = key.encode("utf-8", "surrogateescape")
        n = len(key)
        return self._x(None, b"".join((_INCR, _BULK_LEN[n] if n < _TABLE else b"$%d\r\n" % n, key, _CRLF)))

    def decr(self, key: KeyT) -> Any:
        return self._c(None, "DECR", key)

    def incrby(self, key: KeyT, amount: int = 1, max: Optional[int] = None) -> Any:  # noqa: A002
        """``INCRBY [MAX limit]``. With ``max`` the reply is None (nothing written) when the
        result would pass the limit."""
        if max is None:
            return self._c(None, "INCRBY", key, amount)
        return self._c(None, "INCRBY", key, amount, "MAX", max)

    def decrby(self, key: KeyT, amount: int = 1) -> Any:
        return self._c(None, "DECRBY", key, amount)

    def incrbyfloat(self, key: KeyT, amount: float) -> Any:
        return self._c(float, "INCRBYFLOAT", key, amount)

    def setex(self, key: KeyT, seconds: int, value: Arg) -> Any:
        return self._c(_ok, "SETEX", key, seconds, value)

    def psetex(self, key: KeyT, ms: int, value: Arg) -> Any:
        return self._c(_ok, "PSETEX", key, ms, value)

    def setnx(self, key: KeyT, value: Arg) -> Any:
        return self._c(_bool, "SETNX", key, value)

    def getdel(self, key: KeyT) -> Any:
        return self._c(None, "GETDEL", key)

    def getrange(self, key: KeyT, start: int, end: int) -> Any:
        return self._c(None, "GETRANGE", key, start, end)

    def substr(self, key: KeyT, start: int, end: int) -> Any:
        return self._c(None, "SUBSTR", key, start, end)

    def setrange(self, key: KeyT, offset: int, value: Arg) -> Any:
        return self._c(None, "SETRANGE", key, offset, value)

    def fillget(self, key: KeyT, fill_ms: int, wait_ms: Optional[int] = None) -> Any:
        """``FILLGET key fill_ms [WAIT ms]`` -> :class:`FillResult`. ``WAIT`` parks the whole
        connection; prefer :meth:`get_or_create` which polls without WAIT."""
        if wait_ms is None:
            return self._c(_fill, "FILLGET", key, fill_ms)
        return self._c(_fill, "FILLGET", key, fill_ms, "WAIT", wait_ms)

    def fillset(self, key: KeyT, value: Arg, ttl_ms: int, token: Arg, swr_ms: Optional[int] = None) -> Any:
        """``FILLSET key value ttl_ms token [SWR stale_ms]`` - True when the token was live."""
        if swr_ms is None:
            return self._c(_bool, "FILLSET", key, value, ttl_ms, token)
        return self._c(_bool, "FILLSET", key, value, ttl_ms, token, "SWR", swr_ms)

    # ======================================================= keys / Ruvio primitives
    def limit(self, key: KeyT, max: int, window_ms: int) -> Any:  # noqa: A002
        """``LIMIT key max window_ms`` - remaining allowance, or None when the window is full."""
        return self._c(None, "LIMIT", key, max, window_ms)

    def rate_limit(self, key: KeyT, max: int, window_ms: int) -> Any:  # noqa: A002
        """Helper on ``LIMIT``: ``(allowed, remaining)``."""
        return self._c(_limit, "LIMIT", key, max, window_ms)

    def idem_begin(self, key: KeyT, fingerprint: Arg, owner: Arg, ttl_ms: int) -> Any:
        """``IDEM key BEGIN fingerprint owner ttl_ms`` -> :class:`IdemResult`."""
        return self._c(_idem, "IDEM", key, "BEGIN", fingerprint, owner, ttl_ms)

    def idem_complete(self, key: KeyT, fingerprint: Arg, owner: Arg, result: Arg) -> Any:
        """``IDEM key COMPLETE fingerprint owner result`` - 1 when published."""
        return self._c(None, "IDEM", key, "COMPLETE", fingerprint, owner, result)

    def once(self, key: KeyT, ttl_ms: int) -> Any:
        """``ONCE key ttl_ms`` - True for the first call within the TTL."""
        return self._c(_bool, "ONCE", key, ttl_ms)

    def lease(self, key: KeyT, ttl_ms: int) -> Any:
        """``LEASE key ttl_ms`` - a new fencing token, or None while held."""
        return self._c(None, "LEASE", key, ttl_ms)

    def semaphore(self, key: KeyT, max: int, ttl_ms: int) -> Any:  # noqa: A002
        """``SEMAPHORE key max ttl_ms`` - a holder token, or None when full."""
        return self._c(None, "SEMAPHORE", key, max, ttl_ms)

    def release(self, key: KeyT, token: int) -> Any:
        """``RELEASE key token`` (lease or semaphore) - True when the token was the holder."""
        return self._c(_bool, "RELEASE", key, token)

    def changes_start(self) -> Any:
        """``CHANGES START`` - arms the feed and returns the cursor."""
        return self._c(None, "CHANGES", "START")

    def changes(self, cursor: int) -> Any:
        """``CHANGES cursor`` - ``[(sequence, note), ...]`` after ``cursor``."""
        return self._c(_changes, "CHANGES", cursor)

    def delete(self, *keys: KeyT) -> Any:
        """``DEL`` - number of keys removed."""
        if len(keys) == 1:
            key = keys[0]
            if type(key) is str:
                key = key.encode("utf-8", "surrogateescape")
            if type(key) is bytes:
                n = len(key)
                return self._x(
                    None, b"".join((b"*2\r\n", _DEL, _BULK_LEN[n] if n < _TABLE else b"$%d\r\n" % n, key, _CRLF))
                )
        return self._c(None, "DEL", *keys)

    def unlink(self, *keys: KeyT) -> Any:
        return self._c(None, "UNLINK", *keys)

    def exists(self, *keys: KeyT) -> Any:
        return self._c(None, "EXISTS", *keys)

    def type(self, key: KeyT) -> Any:
        return self._c(None, "TYPE", key)

    def rename(self, src: KeyT, dst: KeyT) -> Any:
        return self._c(_ok, "RENAME", src, dst)

    def renamenx(self, src: KeyT, dst: KeyT) -> Any:
        return self._c(_bool, "RENAMENX", src, dst)

    def scan(
        self, cursor: int = 0, match: Optional[str] = None, count: Optional[int] = None, type: Optional[str] = None  # noqa: A002
    ) -> Any:
        """``SCAN`` page -> ``(next_cursor, keys)``. See :meth:`scan_iter`."""
        args: List[Any] = [cursor]
        if match is not None:
            args += ["MATCH", match]
        if count is not None:
            args += ["COUNT", count]
        if type is not None:
            args += ["TYPE", type]
        return self._c(_scan, "SCAN", *args)

    def dbsize(self) -> Any:
        return self._c(None, "DBSIZE")

    def flushdb(self, asynchronous: bool = False) -> Any:
        return self._c(_ok, "FLUSHDB", "ASYNC") if asynchronous else self._c(_ok, "FLUSHDB")

    def flushall(self, asynchronous: bool = False) -> Any:
        return self._c(_ok, "FLUSHALL", "ASYNC") if asynchronous else self._c(_ok, "FLUSHALL")

    def swapdb(self, first: int, second: int) -> Any:
        return self._c(_ok, "SWAPDB", first, second)

    def move(self, key: KeyT, db: int) -> Any:
        return self._c(_bool, "MOVE", key, db)

    def expire(self, key: KeyT, seconds: int) -> Any:
        return self._c(_bool, "EXPIRE", key, seconds)

    def pexpire(self, key: KeyT, ms: int) -> Any:
        return self._c(_bool, "PEXPIRE", key, ms)

    def expireat(self, key: KeyT, when: int) -> Any:
        return self._c(_bool, "EXPIREAT", key, when)

    def pexpireat(self, key: KeyT, when_ms: int) -> Any:
        return self._c(_bool, "PEXPIREAT", key, when_ms)

    def ttl(self, key: KeyT) -> Any:
        return self._c(None, "TTL", key)

    def pttl(self, key: KeyT) -> Any:
        return self._c(None, "PTTL", key)

    def persist(self, key: KeyT) -> Any:
        return self._c(_bool, "PERSIST", key)

    def expiretime(self, key: KeyT) -> Any:
        return self._c(None, "EXPIRETIME", key)

    def pexpiretime(self, key: KeyT) -> Any:
        return self._c(None, "PEXPIRETIME", key)

    def keys(self, pattern: Arg = "*") -> Any:
        return self._c(None, "KEYS", pattern)

    def randomkey(self) -> Any:
        return self._c(None, "RANDOMKEY")

    def touch(self, *keys: KeyT) -> Any:
        return self._c(None, "TOUCH", *keys)

    def copy(self, src: KeyT, dst: KeyT, replace: bool = False) -> Any:
        return self._c(_bool, "COPY", src, dst, "REPLACE") if replace else self._c(_bool, "COPY", src, dst)

    def object_encoding(self, key: KeyT) -> Any:
        return self._c(_text, "OBJECT", "ENCODING", key)

    def object_idletime(self, key: KeyT) -> Any:
        return self._c(None, "OBJECT", "IDLETIME", key)

    def object_freq(self, key: KeyT) -> Any:
        return self._c(None, "OBJECT", "FREQ", key)

    def memory_usage(self, key: KeyT, samples: Optional[int] = None) -> Any:
        if samples is None:
            return self._c(None, "MEMORY", "USAGE", key)
        return self._c(None, "MEMORY", "USAGE", key, "SAMPLES", samples)

    def dump(self, key: KeyT) -> Any:
        """``DUMP`` - Ruvio's own payload as bytes (always undecoded), or None."""
        return self._x(None, encode("DUMP", (key,)), False)

    def restore(
        self,
        key: KeyT,
        ttl_ms: int,
        payload: bytes,
        replace: bool = False,
        absttl: bool = False,
        idletime: Optional[int] = None,
        freq: Optional[int] = None,
    ) -> Any:
        args: List[Any] = [key, ttl_ms, payload]
        if replace:
            args.append("REPLACE")
        if absttl:
            args.append("ABSTTL")
        if idletime is not None:
            args += ["IDLETIME", idletime]
        if freq is not None:
            args += ["FREQ", freq]
        return self._c(_ok, "RESTORE", *args)

    # ======================================================= lists
    def lpush(self, key: KeyT, *values: Arg) -> Any:
        return self._c(None, "LPUSH", key, *values)

    def rpush(self, key: KeyT, *values: Arg) -> Any:
        return self._c(None, "RPUSH", key, *values)

    def lpushx(self, key: KeyT, *values: Arg) -> Any:
        return self._c(None, "LPUSHX", key, *values)

    def rpushx(self, key: KeyT, *values: Arg) -> Any:
        return self._c(None, "RPUSHX", key, *values)

    def lpushcap(self, key: KeyT, cap: int, *values: Arg) -> Any:
        """``LPUSHCAP key cap value...`` - push to the head, keep at most ``cap`` items."""
        return self._c(None, "LPUSHCAP", key, cap, *values)

    def rpushcap(self, key: KeyT, cap: int, *values: Arg) -> Any:
        """``RPUSHCAP key cap value...`` - push to the tail, keep at most ``cap`` items."""
        return self._c(None, "RPUSHCAP", key, cap, *values)

    def push_recent(self, key: KeyT, value: Arg, keep: int) -> Any:
        """"Recent items" helper: newest first, at most ``keep`` items (``LPUSHCAP``)."""
        return self._c(None, "LPUSHCAP", key, keep, value)

    def lpop(self, key: KeyT, count: Optional[int] = None) -> Any:
        return self._c(None, "LPOP", key) if count is None else self._c(None, "LPOP", key, count)

    def rpop(self, key: KeyT, count: Optional[int] = None) -> Any:
        return self._c(None, "RPOP", key) if count is None else self._c(None, "RPOP", key, count)

    def llen(self, key: KeyT) -> Any:
        return self._c(None, "LLEN", key)

    def lindex(self, key: KeyT, index: int) -> Any:
        return self._c(None, "LINDEX", key, index)

    def lrange(self, key: KeyT, start: int, stop: int) -> Any:
        return self._c(None, "LRANGE", key, start, stop)

    def ltrim(self, key: KeyT, start: int, stop: int) -> Any:
        return self._c(_ok, "LTRIM", key, start, stop)

    def lset(self, key: KeyT, index: int, value: Arg) -> Any:
        return self._c(_ok, "LSET", key, index, value)

    def lrem(self, key: KeyT, count: int, value: Arg) -> Any:
        return self._c(None, "LREM", key, count, value)

    def linsert(self, key: KeyT, where: str, pivot: Arg, value: Arg) -> Any:
        """``where`` is ``BEFORE`` or ``AFTER``."""
        return self._c(None, "LINSERT", key, where, pivot, value)

    def lpos(
        self, key: KeyT, element: Arg, rank: Optional[int] = None, count: Optional[int] = None, maxlen: Optional[int] = None
    ) -> Any:
        args: List[Any] = [key, element]
        if rank is not None:
            args += ["RANK", rank]
        if count is not None:
            args += ["COUNT", count]
        if maxlen is not None:
            args += ["MAXLEN", maxlen]
        return self._c(None, "LPOS", *args)

    def lmove(self, src: KeyT, dst: KeyT, wherefrom: str = "LEFT", whereto: str = "RIGHT") -> Any:
        return self._c(None, "LMOVE", src, dst, wherefrom, whereto)

    def rpoplpush(self, src: KeyT, dst: KeyT) -> Any:
        return self._c(None, "RPOPLPUSH", src, dst)

    def blmove(self, src: KeyT, dst: KeyT, timeout: float, wherefrom: str = "LEFT", whereto: str = "RIGHT") -> Any:
        """Blocking; on the shared asyncio connection it holds every other caller too."""
        return self._c(None, "BLMOVE", src, dst, wherefrom, whereto, timeout)

    def blpop(self, keys: Union[KeyT, Sequence[KeyT]], timeout: float = 0) -> Any:
        """``(key, value)`` or None on timeout. Blocks the connection."""
        return self._c(_key_value, "BLPOP", *_keys(keys), timeout)

    def brpop(self, keys: Union[KeyT, Sequence[KeyT]], timeout: float = 0) -> Any:
        return self._c(_key_value, "BRPOP", *_keys(keys), timeout)

    def lmpop(self, keys: Union[KeyT, Sequence[KeyT]], direction: str = "LEFT", count: Optional[int] = None) -> Any:
        """``(key, [values])`` or None."""
        k = _keys(keys)
        args: List[Any] = [len(k), *k, direction]
        if count is not None:
            args += ["COUNT", count]
        return self._c(_mpop, "LMPOP", *args)

    def blmpop(
        self, timeout: float, keys: Union[KeyT, Sequence[KeyT]], direction: str = "LEFT", count: Optional[int] = None
    ) -> Any:
        k = _keys(keys)
        args: List[Any] = [timeout, len(k), *k, direction]
        if count is not None:
            args += ["COUNT", count]
        return self._c(_mpop, "BLMPOP", *args)

    # ======================================================= sets
    def sadd(self, key: KeyT, *members: Arg) -> Any:
        return self._c(None, "SADD", key, *members)

    def srem(self, key: KeyT, *members: Arg) -> Any:
        return self._c(None, "SREM", key, *members)

    def sismember(self, key: KeyT, member: Arg) -> Any:
        return self._c(_bool, "SISMEMBER", key, member)

    def smismember(self, key: KeyT, *members: Arg) -> Any:
        return self._c(_bools, "SMISMEMBER", key, *members)

    def scard(self, key: KeyT) -> Any:
        return self._c(None, "SCARD", key)

    def smembers(self, key: KeyT) -> Any:
        return self._c(_set, "SMEMBERS", key)

    def sinter(self, *keys: KeyT) -> Any:
        return self._c(_set, "SINTER", *keys)

    def sunion(self, *keys: KeyT) -> Any:
        return self._c(_set, "SUNION", *keys)

    def sdiff(self, *keys: KeyT) -> Any:
        return self._c(_set, "SDIFF", *keys)

    def sinterstore(self, dst: KeyT, *keys: KeyT) -> Any:
        return self._c(None, "SINTERSTORE", dst, *keys)

    def sunionstore(self, dst: KeyT, *keys: KeyT) -> Any:
        return self._c(None, "SUNIONSTORE", dst, *keys)

    def sdiffstore(self, dst: KeyT, *keys: KeyT) -> Any:
        return self._c(None, "SDIFFSTORE", dst, *keys)

    def sintercard(self, keys: Sequence[KeyT], limit: Optional[int] = None) -> Any:
        if limit is None:
            return self._c(None, "SINTERCARD", len(keys), *keys)
        return self._c(None, "SINTERCARD", len(keys), *keys, "LIMIT", limit)

    def smove(self, src: KeyT, dst: KeyT, member: Arg) -> Any:
        return self._c(_bool, "SMOVE", src, dst, member)

    def spop(self, key: KeyT, count: Optional[int] = None) -> Any:
        return self._c(None, "SPOP", key) if count is None else self._c(None, "SPOP", key, count)

    def srandmember(self, key: KeyT, count: Optional[int] = None) -> Any:
        return self._c(None, "SRANDMEMBER", key) if count is None else self._c(None, "SRANDMEMBER", key, count)

    def sscan(self, key: KeyT, cursor: int = 0, match: Optional[str] = None, count: Optional[int] = None) -> Any:
        """``(next_cursor, members)``. See :meth:`sscan_iter`."""
        args: List[Any] = [key, cursor]
        if match is not None:
            args += ["MATCH", match]
        if count is not None:
            args += ["COUNT", count]
        return self._c(_scan, "SSCAN", *args)

    # ======================================================= hashes
    def hset(
        self,
        key: KeyT,
        field: Optional[Arg] = None,
        value: Optional[Arg] = None,
        mapping: Optional[Union[Mapping[Any, Any], Iterable[Tuple[Any, Any]]]] = None,
    ) -> Any:
        """``HSET key field value`` and/or ``mapping`` - number of new fields."""
        if mapping is None:
            return self._c(None, "HSET", key, field, value)
        pairs = _flat_pairs(mapping)
        if field is not None:
            pairs = [field, value, *pairs]
        return self._c(None, "HSET", key, *pairs)

    def hget(self, key: KeyT, field: Arg) -> Any:
        if type(key) is str and type(field) is str:
            k = key.encode("utf-8", "surrogateescape")
            f = field.encode("utf-8", "surrogateescape")
            n = len(k)
            m = len(f)
            if n < _TABLE and m < _TABLE:
                return self._x(None, b"".join((_HGET, _BULK_LEN[n], k, _CRLF, _BULK_LEN[m], f, _CRLF)))
        return self._c(None, "HGET", key, field)

    def hdel(self, key: KeyT, *fields: Arg) -> Any:
        return self._c(None, "HDEL", key, *fields)

    def hlen(self, key: KeyT) -> Any:
        return self._c(None, "HLEN", key)

    def hgetall(self, key: KeyT) -> Any:
        return self._c(_pairs, "HGETALL", key)

    def hmget(self, key: KeyT, *fields: Arg) -> Any:
        if len(fields) == 1 and not isinstance(fields[0], (str, bytes, int, float)):
            fields = tuple(fields[0])  # type: ignore[arg-type]
        return self._c(None, "HMGET", key, *fields)

    def hexists(self, key: KeyT, field: Arg) -> Any:
        return self._c(_bool, "HEXISTS", key, field)

    def hkeys(self, key: KeyT) -> Any:
        return self._c(None, "HKEYS", key)

    def hvals(self, key: KeyT) -> Any:
        return self._c(None, "HVALS", key)

    def hincrby(self, key: KeyT, field: Arg, amount: int = 1) -> Any:
        return self._c(None, "HINCRBY", key, field, amount)

    def hincrbyfloat(self, key: KeyT, field: Arg, amount: float) -> Any:
        return self._c(float, "HINCRBYFLOAT", key, field, amount)

    def hsetnx(self, key: KeyT, field: Arg, value: Arg) -> Any:
        return self._c(_bool, "HSETNX", key, field, value)

    def hstrlen(self, key: KeyT, field: Arg) -> Any:
        return self._c(None, "HSTRLEN", key, field)

    def hscan(self, key: KeyT, cursor: int = 0, match: Optional[str] = None, count: Optional[int] = None) -> Any:
        """``(next_cursor, {field: value})``. See :meth:`hscan_iter`."""
        args: List[Any] = [key, cursor]
        if match is not None:
            args += ["MATCH", match]
        if count is not None:
            args += ["COUNT", count]
        return self._c(_scan_pairs, "HSCAN", *args)

    def hrandfield(self, key: KeyT, count: Optional[int] = None, withvalues: bool = False) -> Any:
        if count is None:
            return self._c(None, "HRANDFIELD", key)
        if withvalues:
            return self._c(lambda r: list(_pairs(r).items()), "HRANDFIELD", key, count, "WITHVALUES")
        return self._c(None, "HRANDFIELD", key, count)

    def _hfield_ttl(self, name: str, key: KeyT, amount: int, fields: Sequence[Arg], cond: List[str]) -> Any:
        return self._c(None, name, key, amount, *cond, "FIELDS", len(fields), *fields)

    def hexpire(
        self, key: KeyT, seconds: int, *fields: Arg, nx: bool = False, xx: bool = False, gt: bool = False, lt: bool = False
    ) -> Any:
        """Per-field TTL. Replies per field: -2 missing, 0 condition not met, 1 set, 2 deleted."""
        return self._hfield_ttl("HEXPIRE", key, seconds, fields, _expire_cond(nx, xx, gt, lt))

    def hpexpire(
        self, key: KeyT, ms: int, *fields: Arg, nx: bool = False, xx: bool = False, gt: bool = False, lt: bool = False
    ) -> Any:
        return self._hfield_ttl("HPEXPIRE", key, ms, fields, _expire_cond(nx, xx, gt, lt))

    def hexpireat(
        self, key: KeyT, when: int, *fields: Arg, nx: bool = False, xx: bool = False, gt: bool = False, lt: bool = False
    ) -> Any:
        return self._hfield_ttl("HEXPIREAT", key, when, fields, _expire_cond(nx, xx, gt, lt))

    def hpexpireat(
        self, key: KeyT, when_ms: int, *fields: Arg, nx: bool = False, xx: bool = False, gt: bool = False, lt: bool = False
    ) -> Any:
        return self._hfield_ttl("HPEXPIREAT", key, when_ms, fields, _expire_cond(nx, xx, gt, lt))

    def httl(self, key: KeyT, *fields: Arg) -> Any:
        return self._c(None, "HTTL", key, "FIELDS", len(fields), *fields)

    def hpttl(self, key: KeyT, *fields: Arg) -> Any:
        return self._c(None, "HPTTL", key, "FIELDS", len(fields), *fields)

    def hexpiretime(self, key: KeyT, *fields: Arg) -> Any:
        return self._c(None, "HEXPIRETIME", key, "FIELDS", len(fields), *fields)

    def hpexpiretime(self, key: KeyT, *fields: Arg) -> Any:
        return self._c(None, "HPEXPIRETIME", key, "FIELDS", len(fields), *fields)

    def hpersist(self, key: KeyT, *fields: Arg) -> Any:
        return self._c(None, "HPERSIST", key, "FIELDS", len(fields), *fields)

    # ======================================================= sorted sets
    def zadd(
        self,
        key: KeyT,
        mapping: Union[Mapping[Any, float], Iterable[Tuple[Any, float]]],
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        ch: bool = False,
        incr: bool = False,
    ) -> Any:
        """``ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member ...`` (mapping is member -> score).
        With ``incr`` the reply is the new score or None."""
        args: List[Any] = [key, *_expire_cond(nx, xx, gt, lt)]
        if ch:
            args.append("CH")
        if incr:
            args.append("INCR")
        items = mapping.items() if isinstance(mapping, Mapping) else mapping
        for member, score in items:
            args.append(score)
            args.append(member)
        return self._c(_float if incr else None, "ZADD", *args)

    def zrange(
        self,
        key: KeyT,
        start: Arg,
        stop: Arg,
        byscore: bool = False,
        bylex: bool = False,
        rev: bool = False,
        offset: Optional[int] = None,
        count: Optional[int] = None,
        withscores: bool = False,
    ) -> Any:
        """``ZRANGE`` - members, or ``[(member, score)]`` with ``withscores``."""
        args: List[Any] = [key, start, stop]
        if byscore:
            args.append("BYSCORE")
        if bylex:
            args.append("BYLEX")
        if rev:
            args.append("REV")
        _limit_args(args, offset, count)
        if withscores:
            args.append("WITHSCORES")
            return self._c(_scored, "ZRANGE", *args)
        return self._c(None, "ZRANGE", *args)

    def zrevrange(self, key: KeyT, start: int, stop: int, withscores: bool = False) -> Any:
        if withscores:
            return self._c(_scored, "ZREVRANGE", key, start, stop, "WITHSCORES")
        return self._c(None, "ZREVRANGE", key, start, stop)

    def zrangebyscore(
        self,
        key: KeyT,
        min: Arg,  # noqa: A002
        max: Arg,  # noqa: A002
        offset: Optional[int] = None,
        count: Optional[int] = None,
        withscores: bool = False,
    ) -> Any:
        args: List[Any] = [key, min, max]
        if withscores:
            args.append("WITHSCORES")
        _limit_args(args, offset, count)
        return self._c(_scored if withscores else None, "ZRANGEBYSCORE", *args)

    def zrangebylex(
        self, key: KeyT, min: Arg, max: Arg, offset: Optional[int] = None, count: Optional[int] = None  # noqa: A002
    ) -> Any:
        args: List[Any] = [key, min, max]
        _limit_args(args, offset, count)
        return self._c(None, "ZRANGEBYLEX", *args)

    def zrevrangebylex(
        self, key: KeyT, max: Arg, min: Arg, offset: Optional[int] = None, count: Optional[int] = None  # noqa: A002
    ) -> Any:
        args: List[Any] = [key, max, min]
        _limit_args(args, offset, count)
        return self._c(None, "ZREVRANGEBYLEX", *args)

    def zrem(self, key: KeyT, *members: Arg) -> Any:
        return self._c(None, "ZREM", key, *members)

    def zincrby(self, key: KeyT, amount: float, member: Arg) -> Any:
        return self._c(float, "ZINCRBY", key, amount, member)

    def zcard(self, key: KeyT) -> Any:
        return self._c(None, "ZCARD", key)

    def zscore(self, key: KeyT, member: Arg) -> Any:
        return self._c(_float, "ZSCORE", key, member)

    def zmscore(self, key: KeyT, *members: Arg) -> Any:
        return self._c(_floats, "ZMSCORE", key, *members)

    def zrank(self, key: KeyT, member: Arg) -> Any:
        return self._c(None, "ZRANK", key, member)

    def zrevrank(self, key: KeyT, member: Arg) -> Any:
        return self._c(None, "ZREVRANK", key, member)

    def zcount(self, key: KeyT, min: Arg, max: Arg) -> Any:  # noqa: A002
        return self._c(None, "ZCOUNT", key, min, max)

    def zlexcount(self, key: KeyT, min: Arg, max: Arg) -> Any:  # noqa: A002
        return self._c(None, "ZLEXCOUNT", key, min, max)

    def zpopmin(self, key: KeyT, count: Optional[int] = None) -> Any:
        """``[(member, score)]``."""
        return self._c(_scored, "ZPOPMIN", key) if count is None else self._c(_scored, "ZPOPMIN", key, count)

    def zpopmax(self, key: KeyT, count: Optional[int] = None) -> Any:
        return self._c(_scored, "ZPOPMAX", key) if count is None else self._c(_scored, "ZPOPMAX", key, count)

    def bzpopmin(self, keys: Union[KeyT, Sequence[KeyT]], timeout: float = 0) -> Any:
        """``(key, member, score)`` or None. Blocks the connection."""
        return self._c(_bzpop, "BZPOPMIN", *_keys(keys), timeout)

    def bzpopmax(self, keys: Union[KeyT, Sequence[KeyT]], timeout: float = 0) -> Any:
        return self._c(_bzpop, "BZPOPMAX", *_keys(keys), timeout)

    def zremrangebyrank(self, key: KeyT, start: int, stop: int) -> Any:
        return self._c(None, "ZREMRANGEBYRANK", key, start, stop)

    def zremrangebyscore(self, key: KeyT, min: Arg, max: Arg) -> Any:  # noqa: A002
        return self._c(None, "ZREMRANGEBYSCORE", key, min, max)

    def zremrangebylex(self, key: KeyT, min: Arg, max: Arg) -> Any:  # noqa: A002
        return self._c(None, "ZREMRANGEBYLEX", key, min, max)

    def _zcombine(
        self,
        name: str,
        dst: Optional[KeyT],
        keys: Sequence[KeyT],
        weights: Optional[Sequence[float]],
        aggregate: Optional[str],
        withscores: bool,
    ) -> Any:
        args: List[Any] = [] if dst is None else [dst]
        args.append(len(keys))
        args += keys
        if weights is not None:
            args.append("WEIGHTS")
            args += weights
        if aggregate is not None:
            args += ["AGGREGATE", aggregate]
        if withscores:
            args.append("WITHSCORES")
            return self._c(_scored, name, *args)
        return self._c(None, name, *args)

    def zunionstore(
        self, dst: KeyT, keys: Sequence[KeyT], weights: Optional[Sequence[float]] = None, aggregate: Optional[str] = None
    ) -> Any:
        return self._zcombine("ZUNIONSTORE", dst, keys, weights, aggregate, False)

    def zinterstore(
        self, dst: KeyT, keys: Sequence[KeyT], weights: Optional[Sequence[float]] = None, aggregate: Optional[str] = None
    ) -> Any:
        return self._zcombine("ZINTERSTORE", dst, keys, weights, aggregate, False)

    def zdiffstore(self, dst: KeyT, keys: Sequence[KeyT]) -> Any:
        return self._zcombine("ZDIFFSTORE", dst, keys, None, None, False)

    def zunion(
        self,
        keys: Sequence[KeyT],
        weights: Optional[Sequence[float]] = None,
        aggregate: Optional[str] = None,
        withscores: bool = False,
    ) -> Any:
        return self._zcombine("ZUNION", None, keys, weights, aggregate, withscores)

    def zinter(
        self,
        keys: Sequence[KeyT],
        weights: Optional[Sequence[float]] = None,
        aggregate: Optional[str] = None,
        withscores: bool = False,
    ) -> Any:
        return self._zcombine("ZINTER", None, keys, weights, aggregate, withscores)

    def zdiff(self, keys: Sequence[KeyT], withscores: bool = False) -> Any:
        return self._zcombine("ZDIFF", None, keys, None, None, withscores)

    def zrandmember(self, key: KeyT, count: Optional[int] = None, withscores: bool = False) -> Any:
        if count is None:
            return self._c(None, "ZRANDMEMBER", key)
        if withscores:
            return self._c(_scored, "ZRANDMEMBER", key, count, "WITHSCORES")
        return self._c(None, "ZRANDMEMBER", key, count)

    def zscan(self, key: KeyT, cursor: int = 0, match: Optional[str] = None, count: Optional[int] = None) -> Any:
        """``(next_cursor, [(member, score)])``. See :meth:`zscan_iter`."""
        args: List[Any] = [key, cursor]
        if match is not None:
            args += ["MATCH", match]
        if count is not None:
            args += ["COUNT", count]
        return self._c(_scan_scored, "ZSCAN", *args)

    # ======================================================= geo
    def geoadd(
        self, key: KeyT, values: Iterable[Tuple[float, float, Arg]], nx: bool = False, xx: bool = False, ch: bool = False
    ) -> Any:
        """``values`` is ``[(longitude, latitude, member), ...]``."""
        args: List[Any] = [key]
        if nx:
            args.append("NX")
        if xx:
            args.append("XX")
        if ch:
            args.append("CH")
        for lon, lat, member in values:
            args += [lon, lat, member]
        return self._c(None, "GEOADD", *args)

    def geodist(self, key: KeyT, member1: Arg, member2: Arg, unit: Optional[str] = None) -> Any:
        if unit is None:
            return self._c(_float, "GEODIST", key, member1, member2)
        return self._c(_float, "GEODIST", key, member1, member2, unit)

    def geohash(self, key: KeyT, *members: Arg) -> Any:
        return self._c(None, "GEOHASH", key, *members)

    def geopos(self, key: KeyT, *members: Arg) -> Any:
        """``[(longitude, latitude) | None]``."""
        return self._c(_geopos, "GEOPOS", key, *members)

    @staticmethod
    def _geo_shape(
        args: List[Any],
        member: Optional[Arg],
        longitude: Optional[float],
        latitude: Optional[float],
        radius: Optional[float],
        width: Optional[float],
        height: Optional[float],
        unit: str,
    ) -> None:
        if member is not None:
            args += ["FROMMEMBER", member]
        else:
            args += ["FROMLONLAT", longitude, latitude]
        if radius is not None:
            args += ["BYRADIUS", radius, unit]
        else:
            args += ["BYBOX", width, height, unit]

    @staticmethod
    def _geo_tail(args: List[Any], sort: Optional[str], count: Optional[int], any: bool) -> None:  # noqa: A002
        if sort is not None:
            args.append(sort)
        if count is not None:
            args += ["COUNT", count]
            if any:
                args.append("ANY")

    def geosearch(
        self,
        key: KeyT,
        member: Optional[Arg] = None,
        longitude: Optional[float] = None,
        latitude: Optional[float] = None,
        radius: Optional[float] = None,
        width: Optional[float] = None,
        height: Optional[float] = None,
        unit: str = "m",
        sort: Optional[str] = None,
        count: Optional[int] = None,
        any: bool = False,  # noqa: A002
        withcoord: bool = False,
        withdist: bool = False,
        withhash: bool = False,
    ) -> Any:
        """``GEOSEARCH`` from a member or lon/lat, by radius or box. Members, or
        :class:`GeoResult` rows when any WITH* flag is set."""
        args: List[Any] = [key]
        self._geo_shape(args, member, longitude, latitude, radius, width, height, unit)
        self._geo_tail(args, sort, count, any)
        if withcoord:
            args.append("WITHCOORD")
        if withdist:
            args.append("WITHDIST")
        if withhash:
            args.append("WITHHASH")
        return self._c(_geo_rows(withdist, withhash, withcoord), "GEOSEARCH", *args)

    def geosearchstore(
        self,
        dst: KeyT,
        src: KeyT,
        member: Optional[Arg] = None,
        longitude: Optional[float] = None,
        latitude: Optional[float] = None,
        radius: Optional[float] = None,
        width: Optional[float] = None,
        height: Optional[float] = None,
        unit: str = "m",
        sort: Optional[str] = None,
        count: Optional[int] = None,
        any: bool = False,  # noqa: A002
        storedist: bool = False,
    ) -> Any:
        args: List[Any] = [dst, src]
        self._geo_shape(args, member, longitude, latitude, radius, width, height, unit)
        self._geo_tail(args, sort, count, any)
        if storedist:
            args.append("STOREDIST")
        return self._c(None, "GEOSEARCHSTORE", *args)

    def _georadius(
        self,
        name: str,
        args: List[Any],
        withcoord: bool,
        withdist: bool,
        withhash: bool,
        count: Optional[int],
        any: bool,  # noqa: A002
        sort: Optional[str],
        store: Optional[KeyT],
        storedist: Optional[KeyT],
    ) -> Any:
        if withcoord:
            args.append("WITHCOORD")
        if withdist:
            args.append("WITHDIST")
        if withhash:
            args.append("WITHHASH")
        self._geo_tail(args, sort, count, any)
        if store is not None:
            args += ["STORE", store]
        if storedist is not None:
            args += ["STOREDIST", storedist]
        if store is not None or storedist is not None:
            return self._c(None, name, *args)
        return self._c(_geo_rows(withdist, withhash, withcoord), name, *args)

    def georadius(
        self,
        key: KeyT,
        longitude: float,
        latitude: float,
        radius: float,
        unit: str = "m",
        withcoord: bool = False,
        withdist: bool = False,
        withhash: bool = False,
        count: Optional[int] = None,
        any: bool = False,  # noqa: A002
        sort: Optional[str] = None,
        store: Optional[KeyT] = None,
        storedist: Optional[KeyT] = None,
    ) -> Any:
        return self._georadius(
            "GEORADIUS", [key, longitude, latitude, radius, unit], withcoord, withdist, withhash, count, any, sort, store, storedist
        )

    def georadiusbymember(
        self,
        key: KeyT,
        member: Arg,
        radius: float,
        unit: str = "m",
        withcoord: bool = False,
        withdist: bool = False,
        withhash: bool = False,
        count: Optional[int] = None,
        any: bool = False,  # noqa: A002
        sort: Optional[str] = None,
        store: Optional[KeyT] = None,
        storedist: Optional[KeyT] = None,
    ) -> Any:
        return self._georadius(
            "GEORADIUSBYMEMBER", [key, member, radius, unit], withcoord, withdist, withhash, count, any, sort, store, storedist
        )

    # ======================================================= bloom
    def bf_reserve(
        self, key: KeyT, error_rate: float, capacity: int, expansion: Optional[int] = None, nonscaling: bool = False
    ) -> Any:
        args: List[Any] = [key, error_rate, capacity]
        if expansion is not None:
            args += ["EXPANSION", expansion]
        if nonscaling:
            args.append("NONSCALING")
        return self._c(_ok, "BF.RESERVE", *args)

    def bf_add(self, key: KeyT, item: Arg) -> Any:
        return self._c(_bool, "BF.ADD", key, item)

    def bf_madd(self, key: KeyT, *items: Arg) -> Any:
        """One bool per item (True = newly added). A full non-scaling filter gives an error element."""
        return self._c(lambda r: [x if isinstance(x, Exception) else x == 1 for x in r], "BF.MADD", key, *items)

    def bf_insert(
        self,
        key: KeyT,
        items: Sequence[Arg],
        capacity: Optional[int] = None,
        error: Optional[float] = None,
        expansion: Optional[int] = None,
        nocreate: bool = False,
        nonscaling: bool = False,
    ) -> Any:
        args: List[Any] = [key]
        if capacity is not None:
            args += ["CAPACITY", capacity]
        if error is not None:
            args += ["ERROR", error]
        if expansion is not None:
            args += ["EXPANSION", expansion]
        if nocreate:
            args.append("NOCREATE")
        if nonscaling:
            args.append("NONSCALING")
        args.append("ITEMS")
        args += items
        return self._c(lambda r: [x if isinstance(x, Exception) else x == 1 for x in r], "BF.INSERT", *args)

    def bf_info(self, key: KeyT, field: Optional[str] = None) -> Any:
        """A dict of all fields, or one field's value (e.g. ``"CAPACITY"``)."""
        if field is None:
            return self._c(_bf_info, "BF.INFO", key)
        return self._c(lambda r: r[0] if type(r) is list and len(r) == 1 else r, "BF.INFO", key, field)

    def bf_card(self, key: KeyT) -> Any:
        return self._c(None, "BF.CARD", key)

    def bf_exists(self, key: KeyT, item: Arg) -> Any:
        return self._c(_bool, "BF.EXISTS", key, item)

    def bf_mexists(self, key: KeyT, *items: Arg) -> Any:
        return self._c(_bools, "BF.MEXISTS", key, *items)

    def seen_before(self, key: KeyT, *items: Arg) -> Any:
        """Bloom "seen before" helper: marks the items and answers, per item, whether it was
        (probably) seen before this call (``BF.MADD`` returned 0)."""
        return self._c(_bloom_seen, "BF.MADD", key, *items)

    # ======================================================= json
    def json_get(self, key: KeyT, path: Optional[str] = None) -> Any:
        """``JSON.GET`` - the JSON text, or None. See :meth:`json_get_obj`."""
        return self._c(None, "JSON.GET", key) if path is None else self._c(None, "JSON.GET", key, path)

    def json_set(self, key: KeyT, path: str, value: Arg, nx: bool = False, xx: bool = False) -> Any:
        """``JSON.SET key path json [NX|XX]`` with ``value`` already JSON text. True when written."""
        if nx:
            return self._c(_ok, "JSON.SET", key, path, value, "NX")
        if xx:
            return self._c(_ok, "JSON.SET", key, path, value, "XX")
        return self._c(_ok, "JSON.SET", key, path, value)

    def json_get_obj(self, key: KeyT, path: Optional[str] = None) -> Any:
        """Typed JSON helper: ``JSON.GET`` decoded with :func:`json.loads` (None when missing)."""
        return self._c(_json_load, "JSON.GET", key) if path is None else self._c(_json_load, "JSON.GET", key, path)

    def json_set_obj(self, key: KeyT, obj: Any, path: str = "$", nx: bool = False, xx: bool = False) -> Any:
        """Typed JSON helper: :func:`json.dumps` then ``JSON.SET``."""
        return self.json_set(key, path, _json.dumps(obj, separators=(",", ":")), nx=nx, xx=xx)

    def json_mget_obj(self, keys: Sequence[KeyT], path: str = "$") -> Any:
        return self._c(lambda r: [_json_load(x) for x in r], "JSON.MGET", *keys, path)

    def json_mget(self, keys: Sequence[KeyT], path: str = "$") -> Any:
        return self._c(None, "JSON.MGET", *keys, path)

    def json_del(self, key: KeyT, path: Optional[str] = None) -> Any:
        return self._c(None, "JSON.DEL", key) if path is None else self._c(None, "JSON.DEL", key, path)

    def json_forget(self, key: KeyT, path: Optional[str] = None) -> Any:
        return self._c(None, "JSON.FORGET", key) if path is None else self._c(None, "JSON.FORGET", key, path)

    def json_merge(self, key: KeyT, path: str, patch: Arg) -> Any:
        """RFC 7396 merge patch (``patch`` is JSON text)."""
        return self._c(_ok, "JSON.MERGE", key, path, patch)

    def jmerge(self, key: KeyT, path: str, patch: Arg) -> Any:
        return self._c(_ok, "JMERGE", key, path, patch)

    def json_type(self, key: KeyT, path: Optional[str] = None) -> Any:
        return self._c(_text, "JSON.TYPE", key) if path is None else self._c(_text, "JSON.TYPE", key, path)

    def json_strlen(self, key: KeyT, path: Optional[str] = None) -> Any:
        return self._c(None, "JSON.STRLEN", key) if path is None else self._c(None, "JSON.STRLEN", key, path)

    def json_arrlen(self, key: KeyT, path: Optional[str] = None) -> Any:
        return self._c(None, "JSON.ARRLEN", key) if path is None else self._c(None, "JSON.ARRLEN", key, path)

    def json_objkeys(self, key: KeyT, path: Optional[str] = None) -> Any:
        return self._c(None, "JSON.OBJKEYS", key) if path is None else self._c(None, "JSON.OBJKEYS", key, path)

    def json_objlen(self, key: KeyT, path: Optional[str] = None) -> Any:
        return self._c(None, "JSON.OBJLEN", key) if path is None else self._c(None, "JSON.OBJLEN", key, path)

    def json_numincrby(self, key: KeyT, path: str, number: float) -> Any:
        """Returns the new number (int or float)."""
        return self._c(_number, "JSON.NUMINCRBY", key, path, number)

    def json_nummultby(self, key: KeyT, path: str, number: float) -> Any:
        return self._c(_number, "JSON.NUMMULTBY", key, path, number)

    def json_toggle(self, key: KeyT, path: str) -> Any:
        """Returns the new boolean."""
        return self._c(_bool, "JSON.TOGGLE", key, path)

    def json_arrappend(self, key: KeyT, path: str, *values: Arg) -> Any:
        """``values`` are JSON texts (``'1'``, ``'"s"'``). Returns the new length."""
        return self._c(None, "JSON.ARRAPPEND", key, path, *values)

    def json_arrinsert(self, key: KeyT, path: str, index: int, *values: Arg) -> Any:
        return self._c(None, "JSON.ARRINSERT", key, path, index, *values)

    def json_arrpop(self, key: KeyT, path: Optional[str] = None, index: Optional[int] = None) -> Any:
        """The popped element as JSON text, or None."""
        if path is None:
            return self._c(None, "JSON.ARRPOP", key)
        if index is None:
            return self._c(None, "JSON.ARRPOP", key, path)
        return self._c(None, "JSON.ARRPOP", key, path, index)

    def json_arrtrim(self, key: KeyT, path: str, start: int, stop: int) -> Any:
        return self._c(None, "JSON.ARRTRIM", key, path, start, stop)

    def json_arrindex(
        self, key: KeyT, path: str, scalar: Arg, start: Optional[int] = None, stop: Optional[int] = None
    ) -> Any:
        """``scalar`` is JSON text (``'2'``, ``'"x"'``). -1 when absent."""
        args: List[Any] = [key, path, scalar]
        if start is not None:
            args.append(start)
            if stop is not None:
                args.append(stop)
        return self._c(None, "JSON.ARRINDEX", *args)

    def json_clear(self, key: KeyT, path: Optional[str] = None) -> Any:
        return self._c(None, "JSON.CLEAR", key) if path is None else self._c(None, "JSON.CLEAR", key, path)

    def json_strappend(self, key: KeyT, path: str, json_string: Arg) -> Any:
        """``json_string`` is a JSON string literal, e.g. ``'"abc"'``."""
        return self._c(None, "JSON.STRAPPEND", key, path, json_string)

    def json_index_create(self, name: str, prefix: str, path: str, type: Optional[str] = None) -> Any:  # noqa: A002
        """``JSON.INDEX CREATE name PREFIX p PATH $.field [TYPE string|number|bool]`` (database 0 only)."""
        if type is None:
            return self._c(_ok, "JSON.INDEX", "CREATE", name, "PREFIX", prefix, "PATH", path)
        return self._c(_ok, "JSON.INDEX", "CREATE", name, "PREFIX", prefix, "PATH", path, "TYPE", type)

    def json_index_drop(self, name: str) -> Any:
        return self._c(None, "JSON.INDEX", "DROP", name)

    def json_index_list(self) -> Any:
        return self._c(None, "JSON.INDEX", "LIST")

    def json_find(self, index: str, value: Arg, count: Optional[int] = None, after: Optional[KeyT] = None) -> Any:
        """``JSON.FIND name VALUE v [COUNT n] [AFTER key]`` - matching keys in ascending order."""
        args: List[Any] = [index, "VALUE", value]
        if count is not None:
            args += ["COUNT", count]
        if after is not None:
            args += ["AFTER", after]
        return self._c(None, "JSON.FIND", *args)

    # ======================================================= streams
    def xadd(
        self,
        key: KeyT,
        fields: Union[Mapping[Any, Any], Iterable[Tuple[Any, Any]]],
        id: str = "*",  # noqa: A002
        maxlen: Optional[int] = None,
        minid: Optional[str] = None,
        approximate: bool = False,
        nomkstream: bool = False,
        delay_ms: Optional[int] = None,
    ) -> Any:
        """``XADD`` - the new id, or None with ``nomkstream`` on a missing key.
        ``delay_ms`` (Ruvio) hides the entry from XREADGROUP for that long."""
        args: List[Any] = [key]
        if nomkstream:
            args.append("NOMKSTREAM")
        if delay_ms is not None:
            args += ["DELAY", delay_ms]
        if maxlen is not None:
            args += ["MAXLEN", "~", maxlen] if approximate else ["MAXLEN", maxlen]
        if minid is not None:
            args += ["MINID", "~", minid] if approximate else ["MINID", minid]
        args.append(id)
        args += _flat_pairs(fields)
        return self._c(_text, "XADD", *args)

    def xlen(self, key: KeyT) -> Any:
        return self._c(None, "XLEN", key)

    def xrange(self, key: KeyT, start: str = "-", end: str = "+", count: Optional[int] = None) -> Any:
        """``[StreamEntry(id, fields)]``."""
        if count is None:
            return self._c(_entries, "XRANGE", key, start, end)
        return self._c(_entries, "XRANGE", key, start, end, "COUNT", count)

    def xrevrange(self, key: KeyT, end: str = "+", start: str = "-", count: Optional[int] = None) -> Any:
        if count is None:
            return self._c(_entries, "XREVRANGE", key, end, start)
        return self._c(_entries, "XREVRANGE", key, end, start, "COUNT", count)

    def xread(self, streams: Mapping[Any, str], count: Optional[int] = None, block: Optional[int] = None) -> Any:
        """``[(key, [StreamEntry])]`` or None on timeout. ``block`` blocks the connection."""
        args: List[Any] = []
        if count is not None:
            args += ["COUNT", count]
        if block is not None:
            args += ["BLOCK", block]
        args.append("STREAMS")
        args += list(streams.keys())
        args += list(streams.values())
        return self._c(_xread, "XREAD", *args)

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: Mapping[Any, str],
        count: Optional[int] = None,
        block: Optional[int] = None,
        noack: bool = False,
    ) -> Any:
        """Ruvio reads one stream with the ``>`` id."""
        args: List[Any] = ["GROUP", group, consumer]
        if count is not None:
            args += ["COUNT", count]
        if block is not None:
            args += ["BLOCK", block]
        if noack:
            args.append("NOACK")
        args.append("STREAMS")
        args += list(streams.keys())
        args += list(streams.values())
        return self._c(_xread, "XREADGROUP", *args)

    def xdel(self, key: KeyT, *ids: str) -> Any:
        return self._c(None, "XDEL", key, *ids)

    def xsetid(self, key: KeyT, last_id: str) -> Any:
        return self._c(_ok, "XSETID", key, last_id)

    def xtrim(self, key: KeyT, maxlen: Optional[int] = None, minid: Optional[str] = None, approximate: bool = False) -> Any:
        if maxlen is not None:
            return self._c(None, "XTRIM", key, "MAXLEN", *(("~", maxlen) if approximate else (maxlen,)))
        return self._c(None, "XTRIM", key, "MINID", *(("~", minid) if approximate else (minid,)))

    def xgroup_create(self, key: KeyT, group: str, id: str = "$", mkstream: bool = False) -> Any:  # noqa: A002
        if mkstream:
            return self._c(_ok, "XGROUP CREATE", key, group, id, "MKSTREAM")
        return self._c(_ok, "XGROUP CREATE", key, group, id)

    def xgroup_createconsumer(self, key: KeyT, group: str, consumer: str) -> Any:
        return self._c(_bool, "XGROUP CREATECONSUMER", key, group, consumer)

    def xgroup_destroy(self, key: KeyT, group: str) -> Any:
        return self._c(_bool, "XGROUP DESTROY", key, group)

    def xgroup_setid(self, key: KeyT, group: str, id: str) -> Any:  # noqa: A002
        return self._c(_ok, "XGROUP SETID", key, group, id)

    def xgroup_delconsumer(self, key: KeyT, group: str, consumer: str) -> Any:
        return self._c(None, "XGROUP DELCONSUMER", key, group, consumer)

    def xack(self, key: KeyT, group: str, *ids: str) -> Any:
        return self._c(None, "XACK", key, group, *ids)

    def xpending(self, key: KeyT, group: str) -> Any:
        """Summary form -> :class:`PendingSummary`."""
        return self._c(_xpending, "XPENDING", key, group)

    def xpending_range(
        self,
        key: KeyT,
        group: str,
        start: str = "-",
        end: str = "+",
        count: int = 100,
        consumer: Optional[str] = None,
        idle: Optional[int] = None,
    ) -> Any:
        """Extended ``XPENDING`` -> ``[PendingEntry]``."""
        args: List[Any] = [key, group]
        if idle is not None:
            args += ["IDLE", idle]
        args += [start, end, count]
        if consumer is not None:
            args.append(consumer)
        return self._c(_xpending_range, "XPENDING", *args)

    def xautoclaim(
        self,
        key: KeyT,
        group: str,
        consumer: str,
        min_idle_ms: int,
        start: str = "0-0",
        count: Optional[int] = None,
        justid: bool = False,
    ) -> Any:
        args: List[Any] = [key, group, consumer, min_idle_ms, start]
        if count is not None:
            args += ["COUNT", count]
        if justid:
            args.append("JUSTID")
        return self._c(_xautoclaim, "XAUTOCLAIM", *args)

    def xclaim(
        self,
        key: KeyT,
        group: str,
        consumer: str,
        min_idle_ms: int,
        ids: Sequence[str],
        idle: Optional[int] = None,
        time: Optional[int] = None,
        retrycount: Optional[int] = None,
        force: bool = False,
        justid: bool = False,
        lastid: Optional[str] = None,
    ) -> Any:
        args: List[Any] = [key, group, consumer, min_idle_ms, *ids]
        if idle is not None:
            args += ["IDLE", idle]
        if time is not None:
            args += ["TIME", time]
        if retrycount is not None:
            args += ["RETRYCOUNT", retrycount]
        if force:
            args.append("FORCE")
        if justid:
            args.append("JUSTID")
        if lastid is not None:
            args += ["LASTID", lastid]
        return self._c(_xclaim, "XCLAIM", *args)

    def xinfo_stream(self, key: KeyT) -> Any:
        return self._c(_xinfo_stream, "XINFO STREAM", key)

    def xinfo_groups(self, key: KeyT) -> Any:
        return self._c(_list_of_dicts, "XINFO GROUPS", key)

    def xinfo_consumers(self, key: KeyT, group: str) -> Any:
        return self._c(_list_of_dicts, "XINFO CONSUMERS", key, group)

    # ======================================================= server / introspection
    def info(self, section: Optional[str] = None) -> Any:
        """Raw ``INFO`` text (``info("prometheus")`` for Prometheus text). See :func:`parse_info`."""
        return self._c(_info, "INFO") if section is None else self._c(_info, "INFO", section)

    def wait(self, numreplicas: int, timeout_ms: int) -> Any:
        return self._c(None, "WAIT", numreplicas, timeout_ms)

    def config_get(self, pattern: str = "*") -> Any:
        return self._c(_pairs_text_keys, "CONFIG GET", pattern)

    def save(self) -> Any:
        return self._c(_ok, "SAVE")

    def bgsave(self) -> Any:
        return self._c(None, "BGSAVE")

    # ======================================================= transactions (raw)
    def multi(self) -> Any:
        """Raw ``MULTI``. Prefer ``pipeline(transaction=True)``; on the shared asyncio
        connection raw MULTI/WATCH interleave with other callers - use ``dedicated()``."""
        return self._c(_ok, "MULTI")

    def exec(self) -> Any:
        return self._c(None, "EXEC")

    def discard(self) -> Any:
        return self._c(_ok, "DISCARD")

    def watch(self, *keys: KeyT) -> Any:
        return self._c(_ok, "WATCH", *keys)

    def unwatch(self) -> Any:
        return self._c(_ok, "UNWATCH")

    # ======================================================= scripts / functions
    def eval(self, script: str, keys: Sequence[KeyT] = (), args: Sequence[Arg] = ()) -> Any:
        return self._c(None, "EVAL", script, len(keys), *keys, *args)

    def evalsha(self, sha: str, keys: Sequence[KeyT] = (), args: Sequence[Arg] = ()) -> Any:
        return self._c(None, "EVALSHA", sha, len(keys), *keys, *args)

    def script_load(self, script: str) -> Any:
        return self._c(_text, "SCRIPT LOAD", script)

    def script_exists(self, *shas: str) -> Any:
        return self._c(_bools, "SCRIPT EXISTS", *shas)

    def script_flush(self) -> Any:
        return self._c(_ok, "SCRIPT FLUSH")

    def script_kill(self) -> Any:
        return self._c(_ok, "SCRIPT KILL")

    def fcall(self, function: str, keys: Sequence[KeyT] = (), args: Sequence[Arg] = ()) -> Any:
        return self._c(None, "FCALL", function, len(keys), *keys, *args)

    def function_load(self, name: str, body: str) -> Any:
        """``FUNCTION LOAD name body`` (Ruvio: one named Lua body, memory only)."""
        return self._c(_text, "FUNCTION LOAD", name, body)

    def function_list(self) -> Any:
        return self._c(None, "FUNCTION LIST")

    def function_delete(self, name: str) -> Any:
        return self._c(_bool, "FUNCTION DELETE", name)

    # ======================================================= cluster
    def cluster_slots(self) -> Any:
        return self._c(None, "CLUSTER SLOTS")

    def cluster_nodes(self) -> Any:
        return self._c(_info, "CLUSTER NODES")

    def cluster_shards(self) -> Any:
        return self._c(None, "CLUSTER SHARDS")

    def cluster_info(self) -> Any:
        return self._c(_info, "CLUSTER INFO")

    def cluster_myid(self) -> Any:
        return self._c(_text, "CLUSTER MYID")

    def cluster_keyslot(self, key: KeyT) -> Any:
        return self._c(None, "CLUSTER KEYSLOT", key)

    def readonly(self) -> Any:
        return self._c(_ok, "READONLY")

    def readwrite(self) -> Any:
        return self._c(_ok, "READWRITE")

    # ======================================================= pub/sub (publishers)
    def publish(self, channel: Arg, message: Arg) -> Any:
        return self._c(None, "PUBLISH", channel, message)

    def spublish(self, channel: Arg, message: Arg) -> Any:
        return self._c(None, "SPUBLISH", channel, message)

    # ======================================================= bitmaps / hyperloglog
    def setbit(self, key: KeyT, offset: int, value: int) -> Any:
        return self._c(None, "SETBIT", key, offset, value)

    def getbit(self, key: KeyT, offset: int) -> Any:
        return self._c(None, "GETBIT", key, offset)

    def bitcount(self, key: KeyT, start: Optional[int] = None, end: Optional[int] = None, mode: Optional[str] = None) -> Any:
        if start is None:
            return self._c(None, "BITCOUNT", key)
        if mode is None:
            return self._c(None, "BITCOUNT", key, start, end)
        return self._c(None, "BITCOUNT", key, start, end, mode)

    def bitpos(self, key: KeyT, bit: int, start: Optional[int] = None, end: Optional[int] = None) -> Any:
        args: List[Any] = [key, bit]
        if start is not None:
            args.append(start)
            if end is not None:
                args.append(end)
        return self._c(None, "BITPOS", *args)

    def bitop(self, operation: str, dst: KeyT, *keys: KeyT) -> Any:
        return self._c(None, "BITOP", operation, dst, *keys)

    def bitfield(self, key: KeyT, *ops: Arg) -> Any:
        """Raw operation words, e.g. ``bitfield(k, "OVERFLOW", "SAT", "INCRBY", "u8", 0, 10)``."""
        return self._c(None, "BITFIELD", key, *ops)

    def bitfield_ro(self, key: KeyT, *gets: Tuple[str, Arg]) -> Any:
        """``gets`` is ``[(type, offset)]`` e.g. ``("u8", 0)``."""
        args: List[Any] = [key]
        for typ, off in gets:
            args += ["GET", typ, off]
        return self._c(None, "BITFIELD_RO", *args)

    def pfadd(self, key: KeyT, *elements: Arg) -> Any:
        return self._c(_bool, "PFADD", key, *elements)

    def pfcount(self, *keys: KeyT) -> Any:
        return self._c(None, "PFCOUNT", *keys)

    def pfmerge(self, dst: KeyT, *keys: KeyT) -> Any:
        return self._c(_ok, "PFMERGE", dst, *keys)


def parse_info(text: str) -> Dict[str, Any]:
    """Parses ``INFO`` text into ``{field: value}`` (ints where possible)."""
    out: Dict[str, Any] = {}
    for line in text.splitlines():
        if not line or line.startswith("#") or ":" not in line:
            continue
        k, v = line.split(":", 1)
        try:
            out[k] = int(v)
        except ValueError:
            try:
                out[k] = float(v)
            except ValueError:
                out[k] = v
    return out
