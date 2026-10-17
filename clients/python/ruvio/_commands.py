# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Typed Ruvio commands shared by the sync client, the asyncio client and pipelines.

Every public method of :class:`Commands` sends one server command (named in the first line
of its docstring) and converts the reply to a Python value. The same methods exist on
``ruvio.Ruvio``, ``ruvio.asyncio.Ruvio`` (awaitable), ``ruvio.ClusterRuvio`` and on
pipelines (queued, return the pipeline). Use ``help(ruvio.Ruvio.<method>)`` or your IDE
hover to read the per-command notes: arguments, return value, errors and an example.
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
    """Typed Ruvio commands: one method per server command, with converted replies.

    The methods on this class are shared by every client flavour:

    * ``ruvio.Ruvio`` (sync): a method sends the command and returns the converted reply.
    * ``ruvio.asyncio.Ruvio``: the same methods return an awaitable, so write
      ``value = await client.get("k")``. The examples in these docstrings use the sync form;
      add ``await`` on the asyncio client.
    * ``client.pipeline()`` / ``client.pipeline(transaction=True)``: a method queues the command
      and returns the pipeline, so calls chain; ``execute()`` sends the batch and returns one
      converted result per command.

    Conventions used by every method:

    * ``KeyT`` is ``str`` or ``bytes`` (a key); ``Arg`` is ``str``, ``bytes``, ``int`` or
      ``float`` (a value). Text is sent as UTF-8; ``bytes`` are sent as they are.
    * Replies are ``str`` when the client was created with ``decode_responses=True`` (the
      default) and ``bytes`` otherwise; ``client.raw`` gives a bytes view of the same connection.
    * A missing key is reported the way the command documents it (``None``, ``0``, ``False`` or an
      empty list); each method says which.
    * Server error replies raise a :class:`ruvio.RuvioError` subclass: ``WRONGTYPE`` raises
      :class:`ruvio.WrongTypeError`, any other ``-ERR`` raises :class:`ruvio.ResponseError`.
      Socket failures raise :class:`ruvio.ConnectionError` or :class:`ruvio.TimeoutError`. These
      are not repeated in every ``Raises:`` section; only errors specific to a command are.
    * Ruvio-specific commands (``setv``, ``take``, ``limit``, ``lease``, ``fillget`` ...) are
      Ruvio extensions and do not exist on a Redis server.

    Cluster notes (see :class:`ruvio.ClusterRuvio`): keys are routed to the shard that owns their
    hash slot. Keyless admin commands such as ``info`` or ``config_get`` go to the seed node
    only. A server started with ``port_mode = "per-shard"`` (the default for several shards)
    answers ``dbsize``, ``keys``, ``flushdb`` and ``flushall`` for the shard behind the port
    only; the cluster client fans these out to every shard itself.
    """

    __slots__ = ()

    def _x(self, conv: Conv, data: bytes, decode: Optional[bool] = None) -> Any:  # pragma: no cover
        raise NotImplementedError

    def _c(self, conv: Conv, name: str, *args: Any) -> Any:
        return self._x(conv, encode(name, args))

    def execute_command(self, *args: Arg) -> Any:
        """Sends any command and returns the raw, unconverted reply.

        Use it for commands that have no typed method. The first argument is the command name
        (``str`` or ``bytes``), the rest are its arguments, encoded like any other argument.

        Args:
            *args: Command name followed by its arguments, e.g. ``"SET", "k", "v", "PX", 100``.

        Returns:
            The server reply as parsed: ``str``/``bytes`` for strings, ``int``, ``list``, ``None`` for
            a null reply. No conversion is applied (``"OK"`` stays ``"OK"``, not ``True``).

        Raises:
            ResponseError: The server answered with an error (subclasses such as
                :class:`WrongTypeError` for ``WRONGTYPE``).
            CrossSlotError: Cluster client only, when the keys of the command are in different slots.

        Example:
            client.execute_command("SET", "k", "v", "PX", 100)    # -> "OK"
            client.execute_command("OBJECT", "ENCODING", "k")     # -> "embstr"
        """
        name = args[0]
        if type(name) is not str:
            name = _s(bytes(name) if not isinstance(name, bytes) else name)
        return self._x(None, encode(name, args[1:]))

    # ======================================================= connection
    def ruvio(self, option: Optional[str] = None) -> Any:
        """``RUVIO [OWNER]`` - identity check for a Ruvio server.

        Args:
            option: ``None`` for the plain check, or ``"OWNER"`` to get the one-line description of
                how a key's shard is chosen.

        Returns:
            ``"PONG is taken"`` without an option, a short description text with ``"OWNER"``. A Redis
            server answers with an error, which makes this a cheap way to tell Ruvio from Redis.

        Raises:
            ResponseError: ``option`` is anything other than ``OWNER``.

        Example:
            client.ruvio()    # -> "PONG is taken"
        """
        return self._c(None, "RUVIO") if option is None else self._c(None, "RUVIO", option)

    def ping(self, message: Optional[Arg] = None) -> Any:
        """``PING [message]`` - checks that the connection is alive.

        Args:
            message: Optional text/bytes to be echoed back.

        Returns:
            ``"PONG"`` without a message, otherwise the message itself.

        Example:
            client.ping()          # -> "PONG"
            client.ping("hello")   # -> "hello"
        """
        return self._c(None, "PING") if message is None else self._c(None, "PING", message)

    def echo(self, message: Arg) -> Any:
        """``ECHO message`` - returns ``message`` unchanged.

        Args:
            message: Text or bytes to send back.

        Returns:
            The same message as the server received it.
        """
        return self._c(None, "ECHO", message)

    def hello(
        self,
        protover: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        setname: Optional[str] = None,
    ) -> Any:
        """``HELLO [protover [AUTH user pass] [SETNAME name]]`` - negotiates the protocol version.

        Without arguments it only reports server information. ``HELLO 3`` switches this connection to
        RESP3 (the typed converters accept both RESP2 and RESP3 shapes). ``CLIENT TRACKING ON``
        requires RESP3. Protocol replies of Pub/Sub stay RESP2 arrays.

        Args:
            protover: ``2`` or ``3``, or ``None`` to leave the protocol alone. Any other number is
                refused with ``NOPROTO`` and nothing changes.
            username: ACL user for the ``AUTH`` part (default user when only ``password`` is given).
                Used only together with ``protover`` and ``password``.
            password: Password to authenticate with in the same round trip.
            setname: Connection name to set (see :meth:`client_setname`). Used only with ``protover``.

        Returns:
            A dict with text keys: ``server`` (``"ruvio"``), ``version``, ``proto``, ``id``,
            ``mode``, ``role`` and ``modules``.

        Raises:
            ResponseError: Unknown protocol version (``NOPROTO``) or bad credentials; the previous
                protocol and name stay in place.

        Example:
            info = client.hello(3)
            info["proto"]   # -> 3
        """
        args: List[Any] = []
        if protover is not None:
            args.append(protover)
            if password is not None:
                args += ["AUTH", username or "default", password]
            if setname is not None:
                args += ["SETNAME", setname]
        return self._c(_hello, "HELLO", *args)

    def auth(self, password: str, username: Optional[str] = None) -> Any:
        """``AUTH [username] password`` - authenticates the connection.

        The sync and asyncio ``Ruvio`` clients remember the credentials and send them again after a
        reconnect.

        Args:
            password: The password.
            username: ACL user name. Omit to authenticate the ``default`` user.

        Returns:
            ``True`` on success.

        Raises:
            ResponseError: Wrong credentials, or no password is configured on the server.

        Example:
            client.auth("s3cret")
            client.auth("s3cret", username="app")
        """
        if username is None:
            return self._c(_ok, "AUTH", password)
        return self._c(_ok, "AUTH", username, password)

    def select(self, db: int) -> Any:
        """``SELECT db`` - switches this connection to a logical database.

        The sync and asyncio ``Ruvio`` clients remember the number and select it again after a
        reconnect. Database 0 is the default. A server has ``databases`` numbered key spaces (16 by
        default, at most 256) when it runs one shard; with several shards only database 0 exists.
        The cluster client sends ``SELECT`` to every node.

        Args:
            db: Database number, ``0`` to ``databases - 1``.

        Returns:
            ``True``.

        Raises:
            ResponseError: ``db`` is out of range, or is not ``0`` on a multi-shard (cluster) server.

        Example:
            client.select(1)
        """
        return self._c(_ok, "SELECT", db)

    def command(self) -> Any:
        """``COMMAND`` - lists the commands the server knows.

        Returns:
            The raw reply: a list with one entry per command (Redis 7 shape: name, arity, flags and
            key positions; tips, key specs and subcommands are empty).
        """
        return self._c(None, "COMMAND")

    def command_count(self) -> Any:
        """``COMMAND COUNT`` - number of commands the server knows.

        Returns:
            The count as ``int``.
        """
        return self._c(None, "COMMAND COUNT")

    def command_info(self, *names: str) -> Any:
        """``COMMAND INFO [name ...]`` - table entries of the named commands.

        Args:
            *names: Command names. With none, every command is described.

        Returns:
            The raw reply: one entry per name (``None`` for an unknown name).
        """
        return self._c(None, "COMMAND INFO", *names)

    def command_docs(self, *names: str) -> Any:
        """``COMMAND DOCS [name ...]`` - documentation entries of the named commands.

        Args:
            *names: Command names.

        Returns:
            The raw reply. Ruvio returns empty maps; use these docstrings or the website for docs.
        """
        return self._c(None, "COMMAND DOCS", *names)

    def client_getname(self) -> Any:
        """``CLIENT GETNAME`` - the name of this connection.

        Returns:
            The name set with :meth:`client_setname` / ``HELLO ... SETNAME``, or ``None`` when the
            connection has no name.
        """
        return self._c(None, "CLIENT GETNAME")

    def client_setname(self, name: str) -> Any:
        """``CLIENT SETNAME name`` - names this connection (shown by ``CLIENT LIST``).

        The name is not durable and is sent again by the sync and asyncio clients after a reconnect.

        Args:
            name: The name; an empty string clears it.

        Returns:
            ``True``.
        """
        return self._c(_ok, "CLIENT SETNAME", name)

    def client_tracking(self, on: bool) -> Any:
        """``CLIENT TRACKING ON|OFF`` - turns client-side-caching invalidation on or off.

        Ruvio records the keys read by ``GET`` and ``MGET`` (misses included) on the shard of this
        connection and pushes one RESP3 ``invalidate`` message when such a key is written. Only the
        plain ``ON``/``OFF`` forms are accepted (no ``REDIRECT``, ``BCAST``, ``PREFIX``,
        ``OPTIN``, ``OPTOUT`` or ``NOLOOP``); tracking does not cross shards.

        Args:
            on: ``True`` for ``ON`` (needs RESP3, see :meth:`hello`), ``False`` for ``OFF``.

        Returns:
            ``True``.

        Raises:
            ResponseError: ``ON`` while the connection still speaks RESP2.

        Example:
            client.hello(3)
            client.client_tracking(True)
        """
        return self._c(_ok, "CLIENT TRACKING", "ON" if on else "OFF")

    def client_id(self) -> Any:
        """``CLIENT ID`` - the id of this connection.

        Ids are per shard: with several shards two connections can share an id.

        Returns:
            The id as ``int``.
        """
        return self._c(None, "CLIENT ID")

    def client_list(self, type: Optional[str] = None) -> Any:  # noqa: A002
        """``CLIENT LIST [TYPE type]`` - describes the connections of the shard that got the command.

        Each shard only knows its own connections. Lines report id, addr, laddr, name, age, db,
        multi, resp and user. ``TYPE`` is validated but not used as a filter.

        Args:
            type: Optional connection type (``normal``, ``pubsub`` ...), accepted for compatibility.

        Returns:
            The text of the reply, one line per connection.
        """
        return self._c(_info, "CLIENT LIST") if type is None else self._c(_info, "CLIENT LIST", "TYPE", type)

    def client_info(self) -> Any:
        """``CLIENT INFO`` - the ``CLIENT LIST`` line of this connection.

        Returns:
            The line as text.
        """
        return self._c(_info, "CLIENT INFO")

    def client_kill(
        self, addr: Optional[str] = None, *, id: Optional[int] = None, skipme: Optional[bool] = None, legacy: bool = False
    ) -> Any:  # noqa: A002
        """``CLIENT KILL`` - closes connections of the shard that got the command.

        Filters combine (a connection must match all given ones). ``USER``, ``LADDR`` and ``TYPE``
        filters are not supported.

        Args:
            addr: ``ip:port`` of the connection to close (``ADDR``). With ``legacy=True`` this is the
                only argument and is sent as the old ``CLIENT KILL ip:port`` form.
            id: Connection id to close (``ID``, see :meth:`client_id`).
            skipme: ``True`` to keep the calling connection (the server default when omitted),
                ``False`` to allow closing it.
            legacy: Use the old single-address form; the reply is then ``True`` instead of a count.

        Returns:
            The number of closed connections (``int``); ``True`` for the legacy form.

        Raises:
            ResponseError: ``No such client`` for the legacy form when no connection matched, or an
                unsupported filter.

        Example:
            client.client_kill(id=42)
        """
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
        """``SLOWLOG GET [count]`` - recent slow commands of this shard.

        Only commands that go through the generic dispatcher are timed (not the fast paths of
        ``GET``, ``SET``, ``INCR``, ``PING``); blocking commands are skipped. An entry holds the
        command name and its first key, not every argument. The threshold and size are server
        settings (``slowlog_threshold_micros``, ``slowlog_max_len``).

        Args:
            count: Maximum number of entries, newest first. ``None`` for the server default.

        Returns:
            The raw list of entries.
        """
        return self._c(None, "SLOWLOG GET") if count is None else self._c(None, "SLOWLOG GET", count)

    def slowlog_len(self) -> Any:
        """``SLOWLOG LEN`` - number of entries in this shard's slow log.

        Returns:
            The count as ``int``.
        """
        return self._c(None, "SLOWLOG LEN")

    def slowlog_reset(self) -> Any:
        """``SLOWLOG RESET`` - empties this shard's slow log.

        Returns:
            ``True``.
        """
        return self._c(_ok, "SLOWLOG RESET")

    def latency_latest(self) -> Any:
        """``LATENCY LATEST`` - latency spike events.

        Ruvio records no such events, so the list is always empty; use ``info("latencystats")``.

        Returns:
            ``[]``.
        """
        return self._c(None, "LATENCY LATEST")

    # ======================================================= acl
    def acl_whoami(self) -> Any:
        """``ACL WHOAMI`` - the user this connection is authenticated as.

        Returns:
            The name used at the last ``AUTH`` as text.
        """
        return self._c(_text, "ACL WHOAMI")

    def acl_users(self) -> Any:
        """``ACL USERS`` - the names of all ACL users, including ones added with :meth:`acl_setuser`.

        Returns:
            A list of user names.
        """
        return self._c(_texts, "ACL USERS")

    def acl_list(self) -> Any:
        """``ACL LIST`` - one rule line per ACL user. Passwords are redacted.

        Returns:
            A list of rule strings.
        """
        return self._c(_texts, "ACL LIST")

    def acl_getuser(self, username: str) -> Any:
        """``ACL GETUSER username`` - the rules of one user.

        Args:
            username: The ACL user.

        Returns:
            A dict with text keys ``flags`` (``on``/``off``), ``passwords`` (how many, not the
            passwords), ``commands`` and ``keys`` (the rule text). ``None`` when the user does not exist.
        """
        return self._c(lambda r: None if r is None else _pairs_text_keys(r), "ACL GETUSER", username)

    def acl_cat(self, category: Optional[str] = None) -> Any:
        """``ACL CAT [category]`` - command categories, or the commands in one category.

        Args:
            category: A category name. ``None`` lists the categories (Ruvio categories only).

        Returns:
            A list of names.
        """
        return self._c(_texts, "ACL CAT") if category is None else self._c(_texts, "ACL CAT", category)

    def acl_setuser(self, username: str, *rules: str) -> Any:
        """``ACL SETUSER name rule...`` - creates or changes an ACL user.

        Rules are applied one after the other; a failed rule changes nothing. A new user starts
        with no password, no commands and no keys. Open connections keep the rules from their last
        ``AUTH``. Changes live until restart unless saved with :meth:`acl_save`. Refused while
        replication is enabled.

        Args:
            username: The user to create or change.
            *rules: Rule tokens such as ``"on"``, ``">password"``, ``"+get"``, ``"-@dangerous"``,
                ``"~app:*"``, ``"nopass"``.

        Returns:
            ``True``.

        Raises:
            ResponseError: A rule is not valid.

        Example:
            client.acl_setuser("app", "on", ">pw", "+get", "+set", "~app:*")
        """
        return self._c(_ok, "ACL SETUSER", username, *rules)

    def acl_load(self) -> Any:
        """``ACL LOAD`` - replaces the live users from the ACL file (``security.acl_file``).

        Returns:
            ``True``.

        Raises:
            ResponseError: No ACL file path is configured, or a line is invalid (nothing changes).
        """
        return self._c(_ok, "ACL LOAD")

    def acl_save(self) -> Any:
        """``ACL SAVE`` - writes the live users to the ACL file, passwords as BLAKE3 hashes.

        Returns:
            ``True``.

        Raises:
            ResponseError: No ACL file path is configured.
        """
        return self._c(_ok, "ACL SAVE")

    # ======================================================= strings
    def get(self, key: KeyT) -> Any:
        """``GET key`` - the string value of a key.

        Args:
            key: The key.

        Returns:
            The value, or ``None`` when the key does not exist.

        Raises:
            WrongTypeError: The key holds something other than a string.

        Example:
            client.set("greeting", "hello")
            client.get("greeting")   # -> "hello"
            client.get("missing")    # -> None
        """
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
        """``SET key value [IFEQ x] [EX|PX|EXAT|PXAT|KEEPTTL] [NX|XX] [GET]`` - writes a string.

        ``SET`` replaces a value of any type. Without ``keepttl`` it clears an existing TTL.
        ``nx``, ``xx`` and ``ifeq`` are conditions: when one is not met nothing is written.
        ``ifeq`` is a Ruvio extension: it writes only when the current string equals the given value,
        which makes compare-and-set and lock renewal a single command; it cannot be combined with
        ``nx``, ``xx`` or ``get``.

        Args:
            key: The key.
            value: The value (text, bytes, int or float; numbers are stored as their decimal text).
            ex: Expire after this many seconds (positive integer).
            px: Expire after this many milliseconds (positive integer).
            exat: Expire at this Unix time in seconds. A time in the past leaves the key absent.
            pxat: Expire at this Unix time in milliseconds. A time in the past leaves the key absent.
            keepttl: Keep the TTL the key already has instead of clearing it.
            nx: Write only when the key does not exist.
            xx: Write only when the key already exists.
            get: Return the previous string value instead of a success flag.
            ifeq: Write only when the key holds a string equal to this value (Ruvio extension).

        Returns:
            ``True`` when written, ``False`` when ``nx``/``xx``/``ifeq`` refused the write. With
            ``get=True``: the previous value, or ``None`` when there was none (a refused write then
            also gives the previous value or ``None``).

        Raises:
            WrongTypeError: ``get=True`` or ``ifeq`` was used and the key holds a non-string.
            ResponseError: Invalid or conflicting options (``syntax error``), or a zero/negative TTL.

        Example:
            client.set("k", "v", ex=60)             # -> True, expires in a minute
            client.set("k", "w", nx=True)           # -> False (already exists)
            client.set("k", "x", ifeq="v")          # -> True only if the value is still "v"
            client.set("k", "y", get=True)          # -> previous value
        """
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
        """``SETV key value [version]`` - versioned write (optimistic concurrency on one key). Ruvio extension.

        The first call without ``version`` creates the key at version 1. Every later call must pass the
        version it last saw and receives the next one, so two writers cannot overwrite each other
        silently. A plain ``SET`` replaces a versioned string and removes the version. The version is
        stored only on keys written with ``SETV``.

        Args:
            key: The key.
            value: The new value.
            version: The current version of the key (a non-negative integer). Omit it to create the
                key at version 1.

        Returns:
            The new version number (``int``).

        Raises:
            ResponseError: ``ERR version mismatch`` - ``version`` is not the current one, the key does
                not exist but a version was given, or the key is already versioned and no version was given.
            WrongTypeError: The key holds a non-string type.

        Example:
            v = client.setv("doc", "draft 1")        # -> 1
            v = client.setv("doc", "draft 2", v)     # -> 2
            client.setv("doc", "stale", 1)           # raises ResponseError: version mismatch
        """
        if version is None:
            return self._c(None, "SETV", key, value)
        return self._c(None, "SETV", key, value, version)

    def take(self, key: KeyT, count: int) -> Any:
        """``TAKE key count`` - subtracts ``count`` from an integer key only when enough is left. Ruvio extension.

        Handy for stock, credits and tokens: the check and the subtraction are one atomic step. A
        missing key or a shortfall changes nothing.

        Args:
            key: A key holding a decimal integer.
            count: A positive integer to subtract.

        Returns:
            The remainder after subtracting (``int``, may be ``0``), or ``None`` when the key does not
            exist or holds less than ``count`` (the key is left unchanged).

        Raises:
            ResponseError: ``count`` is not a positive integer, or the value is not an integer.
            WrongTypeError: The key holds a non-string type.

        Example:
            client.set("stock", 5)
            client.take("stock", 2)   # -> 3
            client.take("stock", 9)   # -> None, stock stays 3
        """
        return self._c(None, "TAKE", key, count)

    def delex(self, key: KeyT, ifeq: Arg) -> Any:
        """``DELEX key IFEQ value`` - deletes a string only when it equals ``value``. Ruvio extension.

        Use it to release a lock safely: only the holder who knows the token can delete the key.

        Args:
            key: The key.
            ifeq: The value the key must hold for the delete to happen.

        Returns:
            ``True`` when the key was deleted, ``False`` when it was missing or held another value.

        Raises:
            WrongTypeError: The key holds a non-string type.

        Example:
            client.set("lock", "token-1", px=5000, nx=True)
            client.delex("lock", "token-1")   # -> True
            client.delex("lock", "token-1")   # -> False
        """
        return self._c(_bool, "DELEX", key, "IFEQ", ifeq)

    def getex(self, key: KeyT, ttl_ms: int) -> Any:
        """``GETEX key ttl_ms`` - reads a string and sets its TTL in milliseconds (sliding expiry).

        This is Ruvio's form of ``GETEX``: the TTL is given in milliseconds as a plain number (Redis
        uses ``EX``/``PX``/``PERSIST`` options). ``GET`` never moves a deadline; ``GETEX`` does.

        Args:
            key: The key.
            ttl_ms: New time to live in milliseconds (positive integer).

        Returns:
            The value, or ``None`` when the key does not exist (nothing is stored).

        Raises:
            WrongTypeError: The key holds a non-string type.
            ResponseError: ``ttl_ms`` is zero or not an integer.

        Example:
            client.set("session:1", "data")
            client.getex("session:1", 30000)   # -> "data", now expires in 30 s
        """
        return self._c(None, "GETEX", key, ttl_ms)

    def mget(self, *keys: KeyT) -> Any:
        """``MGET key [key ...]`` - values of several keys.

        Args:
            *keys: The keys, as separate arguments or as one list/tuple.

        Returns:
            A list in the order of ``keys``; an entry is ``None`` when that key is missing or does not
            hold a string (no error for the wrong type).

        Note:
            Keys in different slots are fetched per shard and merged in your order (not atomic). A
            standalone or single-shard server needs no special handling.

        Example:
            client.mset({"a": 1, "b": 2})
            client.mget("a", "b", "c")   # -> ["1", "2", None]
            client.mget(["a", "b"])      # -> ["1", "2"]
        """
        if len(keys) == 1 and not isinstance(keys[0], (str, bytes)):
            keys = tuple(keys[0])  # type: ignore[arg-type]
        return self._c(None, "MGET", *keys)

    def mset(self, mapping: Union[Mapping[Any, Any], Iterable[Tuple[Any, Any]]]) -> Any:
        """``MSET key value [key value ...]`` - writes several strings.

        Existing TTLs are cleared, as with ``SET``. On one server the whole command is applied at once.

        Args:
            mapping: A ``dict`` or an iterable of ``(key, value)`` pairs.

        Returns:
            ``True``.

        Note:
            Pairs for different slots are sent per shard, so the write is not atomic across shards.

        Example:
            client.mset({"a": 1, "b": "two"})
        """
        return self._c(_ok, "MSET", *_flat_pairs(mapping))

    def msetnx(self, mapping: Union[Mapping[Any, Any], Iterable[Tuple[Any, Any]]]) -> Any:
        """``MSETNX key value [key value ...]`` - writes several strings only if none of the keys exists.

        Args:
            mapping: A ``dict`` or an iterable of ``(key, value)`` pairs.

        Returns:
            ``True`` when all keys were written, ``False`` when at least one existed (nothing is written).

        Raises:
            CrossSlotError: Cluster client only - the keys are in different slots (the command is not split).
        """
        return self._c(_bool, "MSETNX", *_flat_pairs(mapping))

    def getset(self, key: KeyT, value: Arg) -> Any:
        """``GETSET key value`` - sets a string and returns the old one. Clears the TTL.

        Args:
            key: The key.
            value: The new value.

        Returns:
            The previous value, or ``None`` when the key did not exist.

        Raises:
            WrongTypeError: The key holds a non-string type.
        """
        return self._c(None, "GETSET", key, value)

    def append(self, key: KeyT, value: Arg) -> Any:
        """``APPEND key value`` - appends to a string (creates it when missing). Keeps an existing TTL.

        Args:
            key: The key.
            value: Text or bytes to add at the end.

        Returns:
            The length of the string after the append, in bytes.

        Raises:
            WrongTypeError: The key holds a non-string type.
        """
        return self._c(None, "APPEND", key, value)

    def strlen(self, key: KeyT) -> Any:
        """``STRLEN key`` - length of a string in bytes.

        Args:
            key: The key.

        Returns:
            The length; ``0`` when the key does not exist.

        Raises:
            WrongTypeError: The key holds a non-string type.
        """
        return self._c(None, "STRLEN", key)

    def incr(self, key: KeyT) -> Any:
        """``INCR key`` - adds 1 to a decimal integer. A missing key counts as 0.

        Counters are signed 64-bit integers and keep an existing TTL.

        Args:
            key: The key.

        Returns:
            The new value (``int``).

        Raises:
            ResponseError: The value is not an integer or the result would overflow (value unchanged).
            WrongTypeError: The key holds a non-string type.

        Example:
            client.incr("hits")   # -> 1
            client.incr("hits")   # -> 2
        """
        if type(key) is str:
            key = key.encode("utf-8", "surrogateescape")
        n = len(key)
        return self._x(None, b"".join((_INCR, _BULK_LEN[n] if n < _TABLE else b"$%d\r\n" % n, key, _CRLF)))

    def decr(self, key: KeyT) -> Any:
        """``DECR key`` - subtracts 1 from a decimal integer. A missing key counts as 0.

        Args:
            key: The key.

        Returns:
            The new value (``int``).

        Raises:
            ResponseError: The value is not an integer or the result would overflow.
            WrongTypeError: The key holds a non-string type.
        """
        return self._c(None, "DECR", key)

    def incrby(self, key: KeyT, amount: int = 1, max: Optional[int] = None) -> Any:  # noqa: A002
        """``INCRBY key amount [MAX limit]`` - adds an integer; with ``max`` it refuses to pass a ceiling.

        The ``MAX`` option is a Ruvio extension, useful for quotas: the check and the increment are
        one atomic step.

        Args:
            key: The key (a missing key counts as 0).
            amount: Integer to add; may be negative.
            max: Optional ceiling. When the result would be greater than ``max`` nothing is written.

        Returns:
            The new value (``int``), or ``None`` when ``max`` would have been exceeded.

        Raises:
            ResponseError: The value is not an integer or the result would overflow 64 bits.
            WrongTypeError: The key holds a non-string type.

        Example:
            client.incrby("quota", 1, max=3)   # -> 1, 2, 3 and then None
        """
        if max is None:
            return self._c(None, "INCRBY", key, amount)
        return self._c(None, "INCRBY", key, amount, "MAX", max)

    def decrby(self, key: KeyT, amount: int = 1) -> Any:
        """``DECRBY key amount`` - subtracts an integer.

        Args:
            key: The key (a missing key counts as 0).
            amount: Integer to subtract.

        Returns:
            The new value (``int``).

        Raises:
            ResponseError: The value is not an integer or the result would overflow.
            WrongTypeError: The key holds a non-string type.
        """
        return self._c(None, "DECRBY", key, amount)

    def incrbyfloat(self, key: KeyT, amount: float) -> Any:
        """``INCRBYFLOAT key amount`` - adds a float. A missing key counts as 0; keeps the TTL.

        The sum is computed in double precision and stored with at most 15 significant digits (Redis
        uses ``long double``, so the last digits can differ).

        Args:
            key: The key.
            amount: Number to add; may be negative.

        Returns:
            The new value as ``float``.

        Raises:
            ResponseError: The value is not a valid float, or the result would be NaN or infinite.
            WrongTypeError: The key holds a non-string type.
        """
        return self._c(float, "INCRBYFLOAT", key, amount)

    def setex(self, key: KeyT, seconds: int, value: Arg) -> Any:
        """``SETEX key seconds value`` - writes a string with a TTL in seconds (same as ``SET ... EX``).

        Args:
            key: The key.
            seconds: Time to live in seconds (positive integer).
            value: The value.

        Returns:
            ``True``.

        Raises:
            ResponseError: ``seconds`` is zero or negative.
        """
        return self._c(_ok, "SETEX", key, seconds, value)

    def psetex(self, key: KeyT, ms: int, value: Arg) -> Any:
        """``PSETEX key ms value`` - writes a string with a TTL in milliseconds (same as ``SET ... PX``).

        Args:
            key: The key.
            ms: Time to live in milliseconds (positive integer).
            value: The value.

        Returns:
            ``True``.

        Raises:
            ResponseError: ``ms`` is zero or negative.
        """
        return self._c(_ok, "PSETEX", key, ms, value)

    def setnx(self, key: KeyT, value: Arg) -> Any:
        """``SETNX key value`` - writes a string only when the key does not exist.

        Args:
            key: The key.
            value: The value.

        Returns:
            ``True`` when written, ``False`` when the key already existed.
        """
        return self._c(_bool, "SETNX", key, value)

    def getdel(self, key: KeyT) -> Any:
        """``GETDEL key`` - reads a string and deletes the key.

        Args:
            key: The key.

        Returns:
            The value, or ``None`` when the key did not exist.

        Raises:
            WrongTypeError: The key holds a non-string type (it is not deleted).
        """
        return self._c(None, "GETDEL", key)

    def getrange(self, key: KeyT, start: int, end: int) -> Any:
        """``GETRANGE key start end`` - a slice of a string by byte offsets, both ends inclusive.

        Args:
            key: The key.
            start: First byte offset; negative counts from the end (``-1`` is the last byte).
            end: Last byte offset, inclusive; negative counts from the end.

        Returns:
            The slice. An empty string when the key is missing or the range is empty. Unlike Redis 7.0, a
            range wholly before the start of the string gives an empty string.

        Raises:
            WrongTypeError: The key holds a non-string type.

        Example:
            client.set("k", "hello")
            client.getrange("k", 1, 3)    # -> "ell"
            client.getrange("k", -2, -1)  # -> "lo"
        """
        return self._c(None, "GETRANGE", key, start, end)

    def substr(self, key: KeyT, start: int, end: int) -> Any:
        """``SUBSTR key start end`` - old name of :meth:`getrange` (same arguments and reply).

        Args:
            key: The key.
            start: First byte offset (negative counts from the end).
            end: Last byte offset, inclusive.

        Returns:
            The slice as text/bytes; empty when the key is missing.
        """
        return self._c(None, "SUBSTR", key, start, end)

    def setrange(self, key: KeyT, offset: int, value: Arg) -> Any:
        """``SETRANGE key offset value`` - overwrites part of a string, growing it with zero bytes if needed.

        The string is capped at 16 MiB and each call rewrites the value (cost grows with its size).
        An existing TTL is kept.

        Args:
            key: The key (created when missing).
            offset: Byte offset to start writing at (``>= 0``).
            value: Text or bytes to write.

        Returns:
            The length of the string after the write, in bytes. An empty ``value`` writes nothing and
            returns the current length.

        Raises:
            ResponseError: The result would be larger than 16 MiB.
            WrongTypeError: The key holds a non-string type.
        """
        return self._c(None, "SETRANGE", key, offset, value)

    def fillget(self, key: KeyT, fill_ms: int, wait_ms: Optional[int] = None) -> Any:
        """``FILLGET key fill_ms [WAIT wait_ms]`` - reads a cache entry and decides who rebuilds it. Ruvio extension.

        This is the read half of cache-stampede protection; :meth:`fillset` is the write half and
        :meth:`get_or_create` wraps both. The reply says what this caller must do:

        * ``HIT``: the value is cached and fresh; use ``value``.
        * ``FILL``: the entry is missing and this caller won the right to build it. Build the value
          and call :meth:`fillset` with ``token`` before ``fill_ms`` elapse.
        * ``BUSY``: another caller is already building it; wait and ask again.
        * ``STALE``: ``value`` is a stale copy inside the stale-while-revalidate window. If ``token`` is
          not ``None`` this caller must refresh it; otherwise someone else already does.

        The fill marker lives in memory only and lapses after ``fill_ms``.

        Args:
            key: The cache key.
            fill_ms: How long the winning caller may take to build the value, in milliseconds.
            wait_ms: Hold a caller that would get ``BUSY`` for up to this many milliseconds (the server
                retries every 5 ms) until the filler writes, the marker lapses (the waiter then becomes
                the filler) or the wait ends with ``BUSY``. This parks the whole connection, so on the
                shared asyncio connection use :meth:`dedicated`; not allowed inside a transaction.

        Returns:
            A :class:`FillResult` ``(state, value, token)``. ``value`` is set for ``HIT`` and ``STALE``,
            ``token`` for ``FILL`` and for a refreshing ``STALE``.

        Raises:
            WrongTypeError: The key holds a non-string type.
            ResponseError: Too many pending fills on the shard.

        Example:
            r = client.fillget("page:home", 5000)
            if r.state == "FILL":
                client.fillset("page:home", render_page(), 60000, r.token)
        """
        if wait_ms is None:
            return self._c(_fill, "FILLGET", key, fill_ms)
        return self._c(_fill, "FILLGET", key, fill_ms, "WAIT", wait_ms)

    def fillset(self, key: KeyT, value: Arg, ttl_ms: int, token: Arg, swr_ms: Optional[int] = None) -> Any:
        """``FILLSET key value ttl_ms token [SWR stale_ms]`` - stores the value built after a ``FILL``. Ruvio extension.

        The write succeeds only while ``token`` is the live fill token of the key, so a slow filler
        whose marker lapsed cannot overwrite the value a newer filler wrote.

        Args:
            key: The cache key.
            value: The value to cache.
            ttl_ms: Fresh time to live in milliseconds.
            token: The token returned by :meth:`fillget` (``FillResult.token``).
            swr_ms: Optional stale-while-revalidate window in milliseconds. The value stays readable
                for this long after ``ttl_ms`` and ``fillget`` reports it as ``STALE`` so one caller can
                refresh it. The soft expiry is not persisted.

        Returns:
            ``True`` when stored, ``False`` when the token was no longer live (nothing is written).

        Raises:
            WrongTypeError: The key holds a non-string type.
        """
        if swr_ms is None:
            return self._c(_bool, "FILLSET", key, value, ttl_ms, token)
        return self._c(_bool, "FILLSET", key, value, ttl_ms, token, "SWR", swr_ms)

    # ======================================================= keys / Ruvio primitives
    def limit(self, key: KeyT, max: int, window_ms: int) -> Any:  # noqa: A002
        """``LIMIT key max window_ms`` - fixed-window rate limiter on one key. Ruvio extension.

        The first call opens a window of ``window_ms`` milliseconds; up to ``max`` calls are allowed
        inside it. A rejected call does not increase the counter. When the window ends the key
        disappears, so an unused limiter costs nothing. See :meth:`rate_limit` for a tuple form.

        Args:
            key: The limiter key (one per user, IP, API key ...).
            max: Allowed calls per window (positive integer).
            window_ms: Window length in milliseconds (positive integer).

        Returns:
            The remaining allowance after this call (``int``, ``0`` for the last allowed call), or
            ``None`` when the window is already full.

        Raises:
            WrongTypeError: The key holds something other than a limiter.
            ResponseError: ``max`` or ``window_ms`` is not a positive integer.

        Example:
            left = client.limit("api:user:7", 100, 60000)
            if left is None:
                ...   # over the limit, reject the request
        """
        return self._c(None, "LIMIT", key, max, window_ms)

    def rate_limit(self, key: KeyT, max: int, window_ms: int) -> Any:  # noqa: A002
        """Rate-limit helper on ``LIMIT``: the same command as :meth:`limit`, returned as a tuple.

        Args:
            key: The limiter key.
            max: Allowed calls per window (positive integer).
            window_ms: Window length in milliseconds (positive integer).

        Returns:
            ``(True, remaining)`` when the call is allowed, ``(False, 0)`` when the window is full.

        Example:
            allowed, remaining = client.rate_limit("login:1.2.3.4", 5, 60000)
        """
        return self._c(_limit, "LIMIT", key, max, window_ms)

    def idem_begin(self, key: KeyT, fingerprint: Arg, owner: Arg, ttl_ms: int) -> Any:
        """``IDEM key BEGIN fingerprint owner ttl_ms`` - starts an idempotent operation. Ruvio extension.

        Use it so a retried request runs its side effect once. :meth:`idempotent` wraps the whole
        flow. The server only compares the tokens; it does not compute a fingerprint. Use a digest of
        all request inputs as ``fingerprint`` and an unguessable per-attempt value as ``owner``, and a
        key scoped to the tenant and operation. Keep the key namespace for this command only: plain
        ``SET``/``DEL`` on it can break the guarantee. This is not exactly-once for external systems.

        Args:
            key: The operation key (for example ``"order:42"``).
            fingerprint: Digest of the request, 1-128 bytes.
            owner: Token of this attempt, 1-128 bytes.
            ttl_ms: Retention in milliseconds, 1 to 604800000 (7 days). Only the call that creates the
                record sets it; later calls never extend it.

        Returns:
            An :class:`IdemResult` ``(state, result)``:

            * ``acquired``: the key was free; this attempt owns it. Run the operation, then call
              :meth:`idem_complete`.
            * ``pending``: the same request is running (even if the owner matches). Do not run it again.
            * ``conflict``: the key holds a different fingerprint.
            * ``completed``: the saved ``result`` is returned for replay.

        Raises:
            ResponseError: Tokens or ``ttl_ms`` out of range, or the key holds data that is not an
                idempotency record.
        """
        return self._c(_idem, "IDEM", key, "BEGIN", fingerprint, owner, ttl_ms)

    def idem_complete(self, key: KeyT, fingerprint: Arg, owner: Arg, result: Arg) -> Any:
        """``IDEM key COMPLETE fingerprint owner result`` - stores the result of an operation. Ruvio extension.

        Args:
            key: The operation key used in :meth:`idem_begin`.
            fingerprint: Same fingerprint as in ``idem_begin``.
            owner: Same owner token as in ``idem_begin``.
            result: The result to replay later (text or bytes, at most 4 MiB). Retention is not extended.

        Returns:
            ``1`` when the result was stored (an identical repeat also returns ``1``), ``0`` when the
            record is missing or expired, the owner/fingerprint differ, or a different result is
            already stored.

        Raises:
            ResponseError: Tokens or ``result`` out of range.
        """
        return self._c(None, "IDEM", key, "COMPLETE", fingerprint, owner, result)

    def once(self, key: KeyT, ttl_ms: int) -> Any:
        """``ONCE key ttl_ms`` - true only for the first call within the TTL. Ruvio extension.

        Use it to send a notification or run a job at most once per period. A later call does not
        refresh the TTL.

        Args:
            key: The marker key.
            ttl_ms: How long the marker lasts, in milliseconds (positive integer).

        Returns:
            ``True`` for the first call, ``False`` until the deadline has passed.

        Raises:
            WrongTypeError: The key holds something other than a ``ONCE`` marker.
        """
        return self._c(_bool, "ONCE", key, ttl_ms)

    def lease(self, key: KeyT, ttl_ms: int) -> Any:
        """``LEASE key ttl_ms`` - takes a lease with a fencing token. Ruvio extension.

        Only one holder at a time. The token grows with every new grant of the same key (it keeps
        counting after expiry or :meth:`release`), so a downstream system can reject writes from an
        older holder. See :meth:`lease_scope` for a ``with`` block.

        Args:
            key: The lease key.
            ttl_ms: Lease duration in milliseconds (positive integer). It is not renewed automatically.

        Returns:
            The fencing token (``int``), or ``None`` while the lease is held by someone else.

        Raises:
            WrongTypeError: The key holds something other than a lease.

        Example:
            token = client.lease("job:nightly", 30000)
            if token is not None:
                try:
                    run_job(token)
                finally:
                    client.release("job:nightly", token)
        """
        return self._c(None, "LEASE", key, ttl_ms)

    def semaphore(self, key: KeyT, max: int, ttl_ms: int) -> Any:  # noqa: A002
        """``SEMAPHORE key max ttl_ms`` - takes one of ``max`` slots. Ruvio extension.

        Each holder has its own TTL. The key goes away when the last holder expires or releases. See
        :meth:`semaphore_scope`.

        Args:
            key: The semaphore key.
            max: Number of concurrent holders allowed (positive integer, at most 4294967295).
            ttl_ms: Hold time of this slot in milliseconds (positive integer).

        Returns:
            A holder token (``int``) while fewer than ``max`` holders are live, or ``None`` when full.

        Raises:
            WrongTypeError: The key holds something other than a semaphore.
        """
        return self._c(None, "SEMAPHORE", key, max, ttl_ms)

    def release(self, key: KeyT, token: int) -> Any:
        """``RELEASE key token`` - frees a lease or a semaphore slot. Ruvio extension.

        Args:
            key: The lease or semaphore key.
            token: The token returned by :meth:`lease` or :meth:`semaphore`.

        Returns:
            ``True`` when the token was a current holder and is released, ``False`` otherwise (the
            holder stays in place).
        """
        return self._c(_bool, "RELEASE", key, token)

    def changes_start(self) -> Any:
        """``CHANGES START`` - arms the in-memory change feed of this shard. Ruvio extension.

        Nothing is recorded before this call and the feed is empty after a restart. For a durable feed
        use ``CHANGES SUBSCRIBE`` with :meth:`execute_command`.

        Returns:
            The starting cursor (``int``) to pass to :meth:`changes`.
        """
        return self._c(None, "CHANGES", "START")

    def changes(self, cursor: int) -> Any:
        """``CHANGES cursor`` - change notes recorded after ``cursor`` on this shard. Ruvio extension.

        Args:
            cursor: A cursor from :meth:`changes_start` or the last sequence number you processed.

        Returns:
            A list of ``(sequence, note)`` pairs, oldest first, at most 1024 per call. Pass the last
            ``sequence`` as the next ``cursor``. An empty list means nothing new.
        """
        return self._c(_changes, "CHANGES", cursor)

    def delete(self, *keys: KeyT) -> Any:
        """``DEL key [key ...]`` - deletes keys of any type.

        Args:
            *keys: One or more keys.

        Returns:
            The number of keys that existed and were removed.

        Note:
            Cluster client: keys in several slots are deleted per shard and the counts are added up (not
            atomic).

        Example:
            client.delete("a", "b")   # -> 2 if both existed
        """
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
        """``UNLINK key [key ...]`` - same as :meth:`delete` (Ruvio frees memory synchronously).

        Args:
            *keys: One or more keys.

        Returns:
            The number of keys removed.
        """
        return self._c(None, "UNLINK", *keys)

    def exists(self, *keys: KeyT) -> Any:
        """``EXISTS key [key ...]`` - counts the given keys that exist.

        Args:
            *keys: One or more keys. A key named twice counts twice.

        Returns:
            How many of the keys exist (``0`` for none).
        """
        return self._c(None, "EXISTS", *keys)

    def type(self, key: KeyT) -> Any:
        """``TYPE key`` - the type name of a key.

        Args:
            key: The key.

        Returns:
            ``"none"`` for a missing key, otherwise ``"string"``, ``"list"``, ``"set"``, ``"zset"``
            (also used by geo), ``"hash"``, ``"stream"``, or a Ruvio type: ``"json"``,
            ``"bloomfilter"``, ``"lease"``, ``"limit"``, ``"semaphore"``, ``"once"``.
        """
        return self._c(None, "TYPE", key)

    def rename(self, src: KeyT, dst: KeyT) -> Any:
        """``RENAME src dst`` - renames a key, replacing ``dst`` if it exists.

        Args:
            src: Existing key.
            dst: New name.

        Returns:
            ``True``.

        Raises:
            ResponseError: ``ERR no such key`` - ``src`` does not exist.
            CrossSlotError: Cluster client - ``src`` and ``dst`` are in different slots (use a shared
                ``{hash tag}``).
        """
        return self._c(_ok, "RENAME", src, dst)

    def renamenx(self, src: KeyT, dst: KeyT) -> Any:
        """``RENAMENX src dst`` - renames a key only if ``dst`` does not exist.

        Args:
            src: Existing key.
            dst: New name.

        Returns:
            ``True`` when renamed, ``False`` when ``dst`` already exists.

        Raises:
            ResponseError: ``src`` does not exist.
            CrossSlotError: Cluster client - the keys are in different slots.
        """
        return self._c(_bool, "RENAMENX", src, dst)

    def scan(
        self, cursor: int = 0, match: Optional[str] = None, count: Optional[int] = None, type: Optional[str] = None  # noqa: A002
    ) -> Any:
        """``SCAN cursor [MATCH pattern] [COUNT n] [TYPE type]`` - one page of key names.

        Start with cursor ``0`` and call again with the returned cursor until it is ``0``;
        :meth:`scan_iter` does that loop. A key present for the whole walk is returned at least once,
        but a key can repeat if the table was rebuilt between calls. ``COUNT`` bounds the work per
        call (about ten slots per requested key), so a page may hold fewer keys than ``count``.

        Args:
            cursor: ``0`` to start, then the cursor of the previous page.
            match: Glob pattern (``*``, ``?``, ``[abc]``) applied to the keys of the page.
            count: Work hint; the server default applies when omitted.
            type: Only keys of this type, as ``TYPE`` reports it (for example ``"string"``).

        Returns:
            ``(next_cursor, keys)``; ``next_cursor == 0`` means the walk is finished.

        Note:
            SCAN is shard-local on the server. ``ClusterRuvio`` walks the shards one after the other and
            folds the shard number into the cursor, so treat the cursor as opaque.

        Example:
            cursor = 0
            while True:
                cursor, keys = client.scan(cursor, match="user:*", count=100)
                handle(keys)
                if cursor == 0:
                    break
        """
        args: List[Any] = [cursor]
        if match is not None:
            args += ["MATCH", match]
        if count is not None:
            args += ["COUNT", count]
        if type is not None:
            args += ["TYPE", type]
        return self._c(_scan, "SCAN", *args)

    def dbsize(self) -> Any:
        """``DBSIZE`` - number of keys in the selected database.

        Includes keys past their TTL that have not been reclaimed yet.

        Returns:
            The count (``int``).

        Note:
            Per-shard server (``port_mode = "per-shard"``): the count is for the shard behind the
            connected port only. ``ClusterRuvio`` adds up all shards.
        """
        return self._c(None, "DBSIZE")

    def flushdb(self, asynchronous: bool = False) -> Any:
        """``FLUSHDB [ASYNC]`` - deletes every key of the selected database.

        Args:
            asynchronous: Sends ``ASYNC``; Ruvio still runs it synchronously.

        Returns:
            ``True``.

        Note:
            Per-shard server: a plain client clears only the shard behind its port. ``ClusterRuvio``
            sends it to every shard (not atomic across shards).
        """
        return self._c(_ok, "FLUSHDB", "ASYNC") if asynchronous else self._c(_ok, "FLUSHDB")

    def flushall(self, asynchronous: bool = False) -> Any:
        """``FLUSHALL [ASYNC]`` - deletes every key of every database.

        Args:
            asynchronous: Sends ``ASYNC``; Ruvio still runs it synchronously.

        Returns:
            ``True``.

        Note:
            Per-shard server: a plain client clears only the shard behind its port. ``ClusterRuvio``
            sends it to every shard.
        """
        return self._c(_ok, "FLUSHALL", "ASYNC") if asynchronous else self._c(_ok, "FLUSHALL")

    def swapdb(self, first: int, second: int) -> Any:
        """``SWAPDB first second`` - swaps two databases in O(1).

        Connections on either database see the other's keys. Does not wake clients blocked on keys
        that now exist. Refused on a multi-shard server.

        Args:
            first: Database number.
            second: Database number.

        Returns:
            ``True``.

        Raises:
            ResponseError: A number is out of range, or the server is sharded.
        """
        return self._c(_ok, "SWAPDB", first, second)

    def move(self, key: KeyT, db: int) -> Any:
        """``MOVE key db`` - moves a key to another database.

        Args:
            key: The key.
            db: Target database number.

        Returns:
            ``True`` when moved, ``False`` when the key is missing or already exists in ``db``.

        Raises:
            ResponseError: ``db`` is the current database, is out of range, or the server is sharded.
        """
        return self._c(_bool, "MOVE", key, db)

    def expire(self, key: KeyT, seconds: int) -> Any:
        """``EXPIRE key seconds`` - sets a TTL in seconds.

        Args:
            key: The key.
            seconds: Time to live. Zero or negative deletes the key (and returns ``True``).

        Returns:
            ``True`` when the key exists and the TTL was set, ``False`` when the key is missing.
        """
        return self._c(_bool, "EXPIRE", key, seconds)

    def pexpire(self, key: KeyT, ms: int) -> Any:
        """``PEXPIRE key ms`` - sets a TTL in milliseconds.

        Args:
            key: The key.
            ms: Time to live. Zero or negative deletes the key (and returns ``True``).

        Returns:
            ``True`` when set, ``False`` when the key is missing.
        """
        return self._c(_bool, "PEXPIRE", key, ms)

    def expireat(self, key: KeyT, when: int) -> Any:
        """``EXPIREAT key when`` - sets an absolute expiry.

        Args:
            key: The key.
            when: Unix time in seconds. A time in the past removes the key.

        Returns:
            ``True`` when set, ``False`` when the key is missing.
        """
        return self._c(_bool, "EXPIREAT", key, when)

    def pexpireat(self, key: KeyT, when_ms: int) -> Any:
        """``PEXPIREAT key when_ms`` - sets an absolute expiry in milliseconds.

        Args:
            key: The key.
            when_ms: Unix time in milliseconds. A time in the past removes the key.

        Returns:
            ``True`` when set, ``False`` when the key is missing.
        """
        return self._c(_bool, "PEXPIREAT", key, when_ms)

    def ttl(self, key: KeyT) -> Any:
        """``TTL key`` - remaining time to live in seconds.

        Args:
            key: The key.

        Returns:
            Seconds left; ``-1`` when the key has no expiry; ``-2`` when it does not exist.
        """
        return self._c(None, "TTL", key)

    def pttl(self, key: KeyT) -> Any:
        """``PTTL key`` - remaining time to live in milliseconds.

        Args:
            key: The key.

        Returns:
            Milliseconds left; ``-1`` when the key has no expiry; ``-2`` when it does not exist.
        """
        return self._c(None, "PTTL", key)

    def persist(self, key: KeyT) -> Any:
        """``PERSIST key`` - removes the TTL of a key.

        Args:
            key: The key.

        Returns:
            ``True`` when a TTL was removed, ``False`` when the key is missing or had none.
        """
        return self._c(_bool, "PERSIST", key)

    def expiretime(self, key: KeyT) -> Any:
        """``EXPIRETIME key`` - absolute expiry as Unix time in seconds.

        Args:
            key: The key.

        Returns:
            The Unix time; ``-1`` when the key has no expiry; ``-2`` when it does not exist.
        """
        return self._c(None, "EXPIRETIME", key)

    def pexpiretime(self, key: KeyT) -> Any:
        """``PEXPIRETIME key`` - absolute expiry as Unix time in milliseconds.

        Args:
            key: The key.

        Returns:
            The Unix time in ms; ``-1`` when no expiry; ``-2`` when the key does not exist.
        """
        return self._c(None, "PEXPIRETIME", key)

    def keys(self, pattern: Arg = "*") -> Any:
        r"""``KEYS pattern`` - all keys matching a glob pattern.

        Walks the whole database in one call, so the server refuses databases with more than 100,000
        keys per shard; use :meth:`scan` there.

        Args:
            pattern: Glob: ``*``, ``?``, ``[abc]``, ``[^a-z]`` and ``\`` escapes. Default ``"*"``.

        Returns:
            A list of key names (empty when nothing matches).

        Raises:
            ResponseError: The database is too large for ``KEYS``.

        Note:
            Per-shard server: only the shard behind the port answers. ``ClusterRuvio`` joins the answers
            of all shards.
        """
        return self._c(None, "KEYS", pattern)

    def randomkey(self) -> Any:
        """``RANDOMKEY`` - a random key of the selected database (not uniform).

        Returns:
            A key name, or ``None`` when the database is empty.
        """
        return self._c(None, "RANDOMKEY")

    def touch(self, *keys: KeyT) -> Any:
        """``TOUCH key [key ...]`` - counts existing keys. Does not change the access time used by eviction.

        Args:
            *keys: One or more keys (same shard).

        Returns:
            How many of the keys exist.
        """
        return self._c(None, "TOUCH", *keys)

    def copy(self, src: KeyT, dst: KeyT, replace: bool = False) -> Any:
        """``COPY src dst [REPLACE]`` - copies a key with its TTL.

        Works for strings, lists, sets, hashes and sorted sets. Streams, JSON, bloom filters and hashes
        with field TTLs are refused. The copy is atomic and costs O(size of the value).

        Args:
            src: Source key.
            dst: Destination key.
            replace: Overwrite ``dst`` when it exists.

        Returns:
            ``True`` when copied, ``False`` when ``src`` is missing or ``dst`` exists without ``replace``.

        Raises:
            ResponseError: The value type cannot be copied.
            CrossSlotError: Cluster client - the keys are in different slots.
        """
        return self._c(_bool, "COPY", src, dst, "REPLACE") if replace else self._c(_bool, "COPY", src, dst)

    def object_encoding(self, key: KeyT) -> Any:
        """``OBJECT ENCODING key`` - the internal encoding Redis 7 would report.

        Args:
            key: The key.

        Returns:
            A name such as ``"int"``, ``"embstr"``, ``"raw"``, ``"listpack"``, ``"hashtable"``,
            ``"skiplist"``, ``"quicklist"``, ``"intset"`` or ``"stream"`` (informational, not the real
            layout); ``None`` when the key is missing.
        """
        return self._c(_text, "OBJECT", "ENCODING", key)

    def object_idletime(self, key: KeyT) -> Any:
        """``OBJECT IDLETIME key`` - seconds since the key was last accessed.

        Needs an LRU ``maxmemory-policy`` on the server.

        Args:
            key: The key.

        Returns:
            Idle seconds (``int``), or ``None`` when the key is missing.

        Raises:
            ResponseError: The server policy does not track idle time.
        """
        return self._c(None, "OBJECT", "IDLETIME", key)

    def object_freq(self, key: KeyT) -> Any:
        """``OBJECT FREQ key`` - access frequency counter of the key.

        Needs ``allkeys-lfu`` as ``maxmemory-policy``.

        Args:
            key: The key.

        Returns:
            The counter (``int``), or ``None`` when the key is missing.

        Raises:
            ResponseError: The server policy does not track frequency.
        """
        return self._c(None, "OBJECT", "FREQ", key)

    def memory_usage(self, key: KeyT, samples: Optional[int] = None) -> Any:
        """``MEMORY USAGE key [SAMPLES n]`` - bytes charged to a key by the server memory budget.

        The size is exact for Ruvio and is not comparable byte for byte with Redis.

        Args:
            key: The key.
            samples: Accepted for compatibility.

        Returns:
            The size in bytes, or ``None`` when the key is missing.
        """
        if samples is None:
            return self._c(None, "MEMORY", "USAGE", key)
        return self._c(None, "MEMORY", "USAGE", key, "SAMPLES", samples)

    def dump(self, key: KeyT) -> Any:
        """``DUMP key`` - serializes a key into Ruvio's own payload (not Redis RDB).

        Only Ruvio's :meth:`restore` can read it. Works for strings, lists, sets, hashes and sorted sets;
        streams, JSON, bloom filters and hashes with field TTLs are refused. The TTL is not included.
        The reply is always returned as ``bytes``.

        Args:
            key: The key.

        Returns:
            The payload (``bytes``), or ``None`` when the key does not exist.

        Example:
            blob = client.dump("k")
            client.restore("k2", 0, blob)
        """
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
        """``RESTORE key ttl_ms payload [REPLACE] [ABSTTL] [IDLETIME n] [FREQ n]`` - creates a key from :meth:`dump`.

        Args:
            key: Key to create.
            ttl_ms: Time to live in milliseconds, ``0`` for none. With ``absttl`` it is a Unix time in ms
                (a time in the past creates nothing).
            payload: ``bytes`` returned by :meth:`dump`.
            replace: Overwrite an existing key.
            absttl: Treat ``ttl_ms`` as an absolute Unix time.
            idletime: Accepted and ignored.
            freq: Accepted and ignored.

        Returns:
            ``True``.

        Raises:
            ResponseError: ``BUSYKEY`` - the key exists and ``replace`` is not set; or the payload is invalid.
        """
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
        """``LPUSH key value [value ...]`` - pushes values onto the head of a list (creates it).

        Values are pushed one after the other, so ``lpush("l", "a", "b")`` leaves ``["b", "a"]``.

        Args:
            key: The list key.
            *values: One or more values.

        Returns:
            The length of the list after the push.

        Raises:
            WrongTypeError: The key holds a non-list type.
        """
        return self._c(None, "LPUSH", key, *values)

    def rpush(self, key: KeyT, *values: Arg) -> Any:
        """``RPUSH key value [value ...]`` - appends values to the tail of a list (creates it).

        Args:
            key: The list key.
            *values: One or more values.

        Returns:
            The length of the list after the push.

        Raises:
            WrongTypeError: The key holds a non-list type.

        Example:
            client.rpush("queue", "a", "b")   # -> 2
        """
        return self._c(None, "RPUSH", key, *values)

    def lpushx(self, key: KeyT, *values: Arg) -> Any:
        """``LPUSHX key value [value ...]`` - like :meth:`lpush` but only when the list already exists.

        Args:
            key: The list key.
            *values: One or more values.

        Returns:
            The new length, or ``0`` when the list does not exist (nothing is created).

        Raises:
            WrongTypeError: The key holds a non-list type.
        """
        return self._c(None, "LPUSHX", key, *values)

    def rpushx(self, key: KeyT, *values: Arg) -> Any:
        """``RPUSHX key value [value ...]`` - like :meth:`rpush` but only when the list already exists.

        Args:
            key: The list key.
            *values: One or more values.

        Returns:
            The new length, or ``0`` when the list does not exist.

        Raises:
            WrongTypeError: The key holds a non-list type.
        """
        return self._c(None, "RPUSHX", key, *values)

    def lpushcap(self, key: KeyT, cap: int, *values: Arg) -> Any:
        """``LPUSHCAP key cap value [value ...]`` - pushes to the head and keeps at most ``cap`` items. Ruvio extension.

        After the push the oldest items (from the tail) are dropped until ``cap`` remain, in one atomic
        step. Useful for "last N events" lists.

        Args:
            key: The list key.
            cap: Maximum list length to keep (positive integer).
            *values: One or more values.

        Returns:
            The length of the list after the push and trim (never more than ``cap``).

        Raises:
            WrongTypeError: The key holds a non-list type.
            ResponseError: ``cap`` is not a positive integer.

        Example:
            client.lpushcap("recent", 100, "event")
        """
        return self._c(None, "LPUSHCAP", key, cap, *values)

    def rpushcap(self, key: KeyT, cap: int, *values: Arg) -> Any:
        """``RPUSHCAP key cap value [value ...]`` - appends to the tail and keeps at most ``cap`` items. Ruvio extension.

        After the push the oldest items (from the head) are dropped until ``cap`` remain.

        Args:
            key: The list key.
            cap: Maximum list length to keep (positive integer).
            *values: One or more values.

        Returns:
            The length of the list after the push and trim (never more than ``cap``).

        Raises:
            WrongTypeError: The key holds a non-list type.
            ResponseError: ``cap`` is not a positive integer.
        """
        return self._c(None, "RPUSHCAP", key, cap, *values)

    def push_recent(self, key: KeyT, value: Arg, keep: int) -> Any:
        """"Recent items" helper: ``LPUSHCAP key keep value`` - newest first, at most ``keep`` items.

        Args:
            key: The list key.
            value: The item to add at the head.
            keep: How many items to keep.

        Returns:
            The list length after the push (at most ``keep``).

        Example:
            client.push_recent("activity:7", "logged in", keep=50)
            client.lrange("activity:7", 0, 9)   # the ten newest
        """
        return self._c(None, "LPUSHCAP", key, keep, value)

    def lpop(self, key: KeyT, count: Optional[int] = None) -> Any:
        """``LPOP key [count]`` - removes and returns elements from the head.

        Args:
            key: The list key.
            count: How many elements to pop (positive integer). Omit for a single element.

        Returns:
            Without ``count``: the element, or ``None`` when the list is empty or missing. With
            ``count``: a list of up to ``count`` elements, or ``None`` when the list is missing.

        Raises:
            WrongTypeError: The key holds a non-list type.
        """
        return self._c(None, "LPOP", key) if count is None else self._c(None, "LPOP", key, count)

    def rpop(self, key: KeyT, count: Optional[int] = None) -> Any:
        """``RPOP key [count]`` - removes and returns elements from the tail.

        Args:
            key: The list key.
            count: How many elements to pop (positive integer). Omit for a single element.

        Returns:
            Without ``count``: the element, or ``None`` when the list is empty or missing. With
            ``count``: a list of up to ``count`` elements, or ``None`` when the list is missing.

        Raises:
            WrongTypeError: The key holds a non-list type.
        """
        return self._c(None, "RPOP", key) if count is None else self._c(None, "RPOP", key, count)

    def llen(self, key: KeyT) -> Any:
        """``LLEN key`` - length of a list.

        Args:
            key: The list key.

        Returns:
            The length; ``0`` when the key does not exist.

        Raises:
            WrongTypeError: The key holds a non-list type.
        """
        return self._c(None, "LLEN", key)

    def lindex(self, key: KeyT, index: int) -> Any:
        """``LINDEX key index`` - the element at a position.

        Args:
            key: The list key.
            index: Zero-based position; negative counts from the tail (``-1`` is the last).

        Returns:
            The element, or ``None`` when the index is out of range or the key is missing.

        Raises:
            WrongTypeError: The key holds a non-list type.
        """
        return self._c(None, "LINDEX", key, index)

    def lrange(self, key: KeyT, start: int, stop: int) -> Any:
        """``LRANGE key start stop`` - a slice of a list, both ends inclusive.

        Args:
            key: The list key.
            start: First position; negative counts from the tail.
            stop: Last position, inclusive (``-1`` for the end).

        Returns:
            A list of elements; empty when the key is missing or the range is empty.

        Raises:
            WrongTypeError: The key holds a non-list type.

        Example:
            client.rpush("l", "a", "b", "c")
            client.lrange("l", 0, -1)   # -> ["a", "b", "c"]
        """
        return self._c(None, "LRANGE", key, start, stop)

    def ltrim(self, key: KeyT, start: int, stop: int) -> Any:
        """``LTRIM key start stop`` - keeps only the elements in the range.

        Args:
            key: The list key.
            start: First position to keep; negative counts from the tail.
            stop: Last position to keep, inclusive.

        Returns:
            ``True``. A range that selects nothing deletes the list.

        Raises:
            WrongTypeError: The key holds a non-list type.
        """
        return self._c(_ok, "LTRIM", key, start, stop)

    def lset(self, key: KeyT, index: int, value: Arg) -> Any:
        """``LSET key index value`` - replaces the element at a position (rewrites the list, O(N)).

        Args:
            key: The list key.
            index: Position; negative counts from the tail.
            value: New element.

        Returns:
            ``True``.

        Raises:
            ResponseError: The key does not exist (``no such key``) or ``index`` is out of range.
            WrongTypeError: The key holds a non-list type.
        """
        return self._c(_ok, "LSET", key, index, value)

    def lrem(self, key: KeyT, count: int, value: Arg) -> Any:
        """``LREM key count value`` - removes matching elements (rewrites the list, O(N)).

        Args:
            key: The list key.
            count: ``> 0`` removes up to that many from the head, ``< 0`` from the tail, ``0``
                removes all matches.
            value: The element to remove.

        Returns:
            The number of removed elements (``0`` when the key is missing).

        Raises:
            WrongTypeError: The key holds a non-list type.
        """
        return self._c(None, "LREM", key, count, value)

    def linsert(self, key: KeyT, where: str, pivot: Arg, value: Arg) -> Any:
        """``LINSERT key BEFORE|AFTER pivot value`` - inserts next to an existing element (O(N)).

        Args:
            key: The list key.
            where: ``"BEFORE"`` or ``"AFTER"``.
            pivot: The existing element to insert next to (first match from the head).
            value: The element to insert.

        Returns:
            The new length, ``-1`` when ``pivot`` is not in the list, ``0`` when the key is missing.

        Raises:
            WrongTypeError: The key holds a non-list type.
        """
        return self._c(None, "LINSERT", key, where, pivot, value)

    def lpos(
        self, key: KeyT, element: Arg, rank: Optional[int] = None, count: Optional[int] = None, maxlen: Optional[int] = None
    ) -> Any:
        """``LPOS key element [RANK n] [COUNT n] [MAXLEN n]`` - position of an element (read-only, O(N)).

        Args:
            key: The list key.
            element: The element to look for.
            rank: Which match to return; ``2`` is the second match, negative scans from the tail.
            count: Return up to this many matches as a list (``0`` for all).
            maxlen: Compare at most this many elements.

        Returns:
            Without ``count``: the zero-based position, or ``None`` when not found. With ``count``: a
            list of positions (empty when none).

        Raises:
            WrongTypeError: The key holds a non-list type.
        """
        args: List[Any] = [key, element]
        if rank is not None:
            args += ["RANK", rank]
        if count is not None:
            args += ["COUNT", count]
        if maxlen is not None:
            args += ["MAXLEN", maxlen]
        return self._c(None, "LPOS", *args)

    def lmove(self, src: KeyT, dst: KeyT, wherefrom: str = "LEFT", whereto: str = "RIGHT") -> Any:
        """``LMOVE src dst LEFT|RIGHT LEFT|RIGHT`` - atomically pops from one list and pushes to another.

        Args:
            src: Source list.
            dst: Destination list (may equal ``src`` to rotate).
            wherefrom: End of ``src`` to pop from, ``"LEFT"`` (head) or ``"RIGHT"`` (tail).
            whereto: End of ``dst`` to push to, ``"LEFT"`` or ``"RIGHT"``.

        Returns:
            The moved element, or ``None`` when ``src`` is empty or missing.

        Raises:
            WrongTypeError: A key holds a non-list type.
            CrossSlotError: Cluster client - the keys are in different slots.
        """
        return self._c(None, "LMOVE", src, dst, wherefrom, whereto)

    def rpoplpush(self, src: KeyT, dst: KeyT) -> Any:
        """``RPOPLPUSH src dst`` - pops the tail of ``src`` and pushes it to the head of ``dst`` (same as ``lmove(src, dst, "RIGHT", "LEFT")``).

        Args:
            src: Source list.
            dst: Destination list.

        Returns:
            The moved element, or ``None`` when ``src`` is empty or missing.

        Raises:
            CrossSlotError: Cluster client - the keys are in different slots.
        """
        return self._c(None, "RPOPLPUSH", src, dst)

    def blmove(self, src: KeyT, dst: KeyT, timeout: float, wherefrom: str = "LEFT", whereto: str = "RIGHT") -> Any:
        """``BLMOVE src dst LEFT|RIGHT LEFT|RIGHT timeout`` - blocking :meth:`lmove`.

        The server polls every 5 ms instead of parking a waiter and counts it against the limit of
        1,024 blocked clients per shard. Not allowed inside a transaction or script. It occupies the
        connection: on the shared asyncio connection every other caller waits too, so run it on
        :meth:`dedicated`. With a sync ``socket_timeout`` shorter than ``timeout`` the call raises
        :class:`TimeoutError`.

        Args:
            src: Source list.
            dst: Destination list.
            timeout: Seconds to wait (float); ``0`` waits until an element arrives.
            wherefrom: ``"LEFT"`` or ``"RIGHT"`` end of ``src``.
            whereto: ``"LEFT"`` or ``"RIGHT"`` end of ``dst``.

        Returns:
            The moved element, or ``None`` when the timeout elapsed.
        """
        return self._c(None, "BLMOVE", src, dst, wherefrom, whereto, timeout)

    def blpop(self, keys: Union[KeyT, Sequence[KeyT]], timeout: float = 0) -> Any:
        """``BLPOP key [key ...] timeout`` - pops from the head of the first non-empty list, waiting if needed.

        Blocks this connection (asyncio: use :meth:`dedicated`). At most 1,024 clients may block per
        shard. All keys must be in one shard/slot.

        Args:
            keys: One key or a list of keys, checked in order.
            timeout: Seconds to wait (float); ``0`` waits until data arrives or the server stops.

        Returns:
            ``(key, value)``, or ``None`` on timeout.

        Example:
            item = client.blpop("jobs", timeout=5)
            if item:
                key, value = item
        """
        return self._c(_key_value, "BLPOP", *_keys(keys), timeout)

    def brpop(self, keys: Union[KeyT, Sequence[KeyT]], timeout: float = 0) -> Any:
        """``BRPOP key [key ...] timeout`` - pops from the tail of the first non-empty list, waiting if needed.

        Same blocking rules as :meth:`blpop`.

        Args:
            keys: One key or a list of keys, checked in order.
            timeout: Seconds to wait (float); ``0`` waits until data arrives.

        Returns:
            ``(key, value)``, or ``None`` on timeout.
        """
        return self._c(_key_value, "BRPOP", *_keys(keys), timeout)

    def lmpop(self, keys: Union[KeyT, Sequence[KeyT]], direction: str = "LEFT", count: Optional[int] = None) -> Any:
        """``LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT n]`` - pops from the first non-empty list.

        Args:
            keys: One key or a list of keys, checked in order.
            direction: ``"LEFT"`` (head) or ``"RIGHT"`` (tail).
            count: Maximum number of elements to pop (default one).

        Returns:
            ``(key, [values])``, or ``None`` when all lists are empty.
        """
        k = _keys(keys)
        args: List[Any] = [len(k), *k, direction]
        if count is not None:
            args += ["COUNT", count]
        return self._c(_mpop, "LMPOP", *args)

    def blmpop(
        self, timeout: float, keys: Union[KeyT, Sequence[KeyT]], direction: str = "LEFT", count: Optional[int] = None
    ) -> Any:
        """``BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT n]`` - blocking :meth:`lmpop`.

        Polls every 5 ms; same blocking rules as :meth:`blmove`.

        Args:
            timeout: Seconds to wait (float); ``0`` waits until data arrives.
            keys: One key or a list of keys, checked in order.
            direction: ``"LEFT"`` or ``"RIGHT"``.
            count: Maximum number of elements to pop.

        Returns:
            ``(key, [values])``, or ``None`` on timeout.
        """
        k = _keys(keys)
        args: List[Any] = [timeout, len(k), *k, direction]
        if count is not None:
            args += ["COUNT", count]
        return self._c(_mpop, "BLMPOP", *args)

    # ======================================================= sets
    def sadd(self, key: KeyT, *members: Arg) -> Any:
        """``SADD key member [member ...]`` - adds members to a set (creates it).

        Args:
            key: The set key.
            *members: One or more members.

        Returns:
            The number of members that were new.

        Raises:
            WrongTypeError: The key holds a non-set type.
        """
        return self._c(None, "SADD", key, *members)

    def srem(self, key: KeyT, *members: Arg) -> Any:
        """``SREM key member [member ...]`` - removes members from a set.

        Args:
            key: The set key.
            *members: One or more members.

        Returns:
            The number of members that were present and removed.

        Raises:
            WrongTypeError: The key holds a non-set type.
        """
        return self._c(None, "SREM", key, *members)

    def sismember(self, key: KeyT, member: Arg) -> Any:
        """``SISMEMBER key member`` - tests membership.

        Args:
            key: The set key.
            member: The member.

        Returns:
            ``True`` if it is a member, ``False`` otherwise (also when the key is missing).
        """
        return self._c(_bool, "SISMEMBER", key, member)

    def smismember(self, key: KeyT, *members: Arg) -> Any:
        """``SMISMEMBER key member [member ...]`` - tests several members at once.

        Args:
            key: The set key.
            *members: Members to test.

        Returns:
            A list of booleans in the order of ``members``.
        """
        return self._c(_bools, "SMISMEMBER", key, *members)

    def scard(self, key: KeyT) -> Any:
        """``SCARD key`` - number of members.

        Args:
            key: The set key.

        Returns:
            The count; ``0`` when the key is missing.
        """
        return self._c(None, "SCARD", key)

    def smembers(self, key: KeyT) -> Any:
        """``SMEMBERS key`` - all members.

        Args:
            key: The set key.

        Returns:
            A Python ``set`` (empty when the key is missing). The server sends them sorted; the
            ``set`` has no order.

        Raises:
            WrongTypeError: The key holds a non-set type.
        """
        return self._c(_set, "SMEMBERS", key)

    def sinter(self, *keys: KeyT) -> Any:
        """``SINTER key [key ...]`` - members present in every set.

        Args:
            *keys: Set keys (same slot on a cluster).

        Returns:
            A ``set``; empty if any key is missing.

        Raises:
            CrossSlotError: Cluster client - the keys are in different slots.
        """
        return self._c(_set, "SINTER", *keys)

    def sunion(self, *keys: KeyT) -> Any:
        """``SUNION key [key ...]`` - members present in any set.

        Args:
            *keys: Set keys (same slot on a cluster).

        Returns:
            A ``set``.

        Raises:
            CrossSlotError: Cluster client - the keys are in different slots.
        """
        return self._c(_set, "SUNION", *keys)

    def sdiff(self, *keys: KeyT) -> Any:
        """``SDIFF key [key ...]`` - members of the first set that are in none of the others.

        Args:
            *keys: Set keys, the first is the base (same slot on a cluster).

        Returns:
            A ``set``.

        Raises:
            CrossSlotError: Cluster client - the keys are in different slots.
        """
        return self._c(_set, "SDIFF", *keys)

    def sinterstore(self, dst: KeyT, *keys: KeyT) -> Any:
        """``SINTERSTORE dst key [key ...]`` - stores the intersection in ``dst``.

        Args:
            dst: Destination key (overwritten; removed if the result is empty).
            *keys: Source set keys.

        Returns:
            The size of the stored set.

        Raises:
            CrossSlotError: Cluster client - the keys are in different slots.
        """
        return self._c(None, "SINTERSTORE", dst, *keys)

    def sunionstore(self, dst: KeyT, *keys: KeyT) -> Any:
        """``SUNIONSTORE dst key [key ...]`` - stores the union in ``dst``.

        Args:
            dst: Destination key (overwritten).
            *keys: Source set keys.

        Returns:
            The size of the stored set.

        Raises:
            CrossSlotError: Cluster client - the keys are in different slots.
        """
        return self._c(None, "SUNIONSTORE", dst, *keys)

    def sdiffstore(self, dst: KeyT, *keys: KeyT) -> Any:
        """``SDIFFSTORE dst key [key ...]`` - stores the difference in ``dst``.

        Args:
            dst: Destination key (overwritten).
            *keys: Source set keys, the first is the base.

        Returns:
            The size of the stored set.

        Raises:
            CrossSlotError: Cluster client - the keys are in different slots.
        """
        return self._c(None, "SDIFFSTORE", dst, *keys)

    def sintercard(self, keys: Sequence[KeyT], limit: Optional[int] = None) -> Any:
        """``SINTERCARD numkeys key [key ...] [LIMIT n]`` - size of the intersection without building it.

        Args:
            keys: A sequence of set keys (same slot on a cluster).
            limit: Stop counting at this many (``0``/``None`` for no limit).

        Returns:
            The intersection size (``int``).
        """
        if limit is None:
            return self._c(None, "SINTERCARD", len(keys), *keys)
        return self._c(None, "SINTERCARD", len(keys), *keys, "LIMIT", limit)

    def smove(self, src: KeyT, dst: KeyT, member: Arg) -> Any:
        """``SMOVE src dst member`` - moves a member from one set to another.

        Args:
            src: Source set.
            dst: Destination set.
            member: The member to move.

        Returns:
            ``True`` if moved, ``False`` if it was not in ``src``.

        Raises:
            CrossSlotError: Cluster client - the keys are in different slots.
        """
        return self._c(_bool, "SMOVE", src, dst, member)

    def spop(self, key: KeyT, count: Optional[int] = None) -> Any:
        """``SPOP key [count]`` - removes and returns random members.

        Args:
            key: The set key.
            count: How many members to pop. Omit for one.

        Returns:
            Without ``count``: a member or ``None`` when the set is empty/missing. With ``count``: a list.
        """
        return self._c(None, "SPOP", key) if count is None else self._c(None, "SPOP", key, count)

    def srandmember(self, key: KeyT, count: Optional[int] = None) -> Any:
        """``SRANDMEMBER key [count]`` - random members without removing them.

        Args:
            key: The set key.
            count: Omit for one member. Positive: up to ``count`` distinct members. Negative: ``-count``
                members that may repeat (limited to 1,048,576). ``0`` gives an empty list.

        Returns:
            Without ``count``: a member or ``None`` when missing. With ``count``: a list. Members of a
            count are near each other in the table, so they are random between calls but not independent draws.
        """
        return self._c(None, "SRANDMEMBER", key) if count is None else self._c(None, "SRANDMEMBER", key, count)

    def sscan(self, key: KeyT, cursor: int = 0, match: Optional[str] = None, count: Optional[int] = None) -> Any:
        """``SSCAN key cursor [MATCH pattern] [COUNT n]`` - one page of set members.

        Start with cursor ``0`` and repeat with the returned cursor until it is ``0`` (see
        :meth:`sscan_iter`). A member can appear twice if the table was rebuilt between pages, but is
        never missed.

        Args:
            key: The set key.
            cursor: ``0`` to start.
            match: Glob pattern for members.
            count: Work hint per page.

        Returns:
            ``(next_cursor, members)``; a missing key gives ``(0, [])``.
        """
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
        """``HSET key field value [field value ...]`` - sets hash fields.

        Give either ``field`` and ``value``, or ``mapping``, or both. Setting a field drops its field TTL.

        Args:
            key: The hash key.
            field: Field name (with ``value``).
            value: Field value.
            mapping: A ``dict`` or iterable of ``(field, value)`` pairs to set as well.

        Returns:
            The number of fields that were new.

        Raises:
            WrongTypeError: The key holds a non-hash type.

        Example:
            client.hset("user:1", mapping={"name": "Ada", "age": 36})   # -> 2
            client.hset("user:1", "city", "London")                     # -> 1
        """
        if mapping is None:
            return self._c(None, "HSET", key, field, value)
        pairs = _flat_pairs(mapping)
        if field is not None:
            pairs = [field, value, *pairs]
        return self._c(None, "HSET", key, *pairs)

    def hget(self, key: KeyT, field: Arg) -> Any:
        """``HGET key field`` - value of one field.

        Args:
            key: The hash key.
            field: Field name.

        Returns:
            The value, or ``None`` when the field or key is missing.

        Raises:
            WrongTypeError: The key holds a non-hash type.
        """
        if type(key) is str and type(field) is str:
            k = key.encode("utf-8", "surrogateescape")
            f = field.encode("utf-8", "surrogateescape")
            n = len(k)
            m = len(f)
            if n < _TABLE and m < _TABLE:
                return self._x(None, b"".join((_HGET, _BULK_LEN[n], k, _CRLF, _BULK_LEN[m], f, _CRLF)))
        return self._c(None, "HGET", key, field)

    def hdel(self, key: KeyT, *fields: Arg) -> Any:
        """``HDEL key field [field ...]`` - deletes fields.

        Args:
            key: The hash key.
            *fields: One or more field names.

        Returns:
            The number of fields removed. The hash disappears with its last field.
        """
        return self._c(None, "HDEL", key, *fields)

    def hlen(self, key: KeyT) -> Any:
        """``HLEN key`` - number of fields.

        Args:
            key: The hash key.

        Returns:
            The count; ``0`` when the key is missing.
        """
        return self._c(None, "HLEN", key)

    def hgetall(self, key: KeyT) -> Any:
        """``HGETALL key`` - all fields and values.

        Args:
            key: The hash key.

        Returns:
            A ``dict`` of field to value (empty when the key is missing). Order is unspecified.

        Raises:
            WrongTypeError: The key holds a non-hash type.
        """
        return self._c(_pairs, "HGETALL", key)

    def hmget(self, key: KeyT, *fields: Arg) -> Any:
        """``HMGET key field [field ...]`` - values of several fields.

        Args:
            key: The hash key.
            *fields: Field names, as arguments or as one list.

        Returns:
            A list in the order of ``fields`` with ``None`` for missing fields.
        """
        if len(fields) == 1 and not isinstance(fields[0], (str, bytes, int, float)):
            fields = tuple(fields[0])  # type: ignore[arg-type]
        return self._c(None, "HMGET", key, *fields)

    def hexists(self, key: KeyT, field: Arg) -> Any:
        """``HEXISTS key field`` - does the field exist.

        Args:
            key: The hash key.
            field: Field name.

        Returns:
            ``True`` or ``False``.
        """
        return self._c(_bool, "HEXISTS", key, field)

    def hkeys(self, key: KeyT) -> Any:
        """``HKEYS key`` - all field names (unspecified order).

        Args:
            key: The hash key.

        Returns:
            A list of names (empty when missing).
        """
        return self._c(None, "HKEYS", key)

    def hvals(self, key: KeyT) -> Any:
        """``HVALS key`` - all values (unspecified order).

        Args:
            key: The hash key.

        Returns:
            A list of values (empty when missing).
        """
        return self._c(None, "HVALS", key)

    def hincrby(self, key: KeyT, field: Arg, amount: int = 1) -> Any:
        """``HINCRBY key field amount`` - adds an integer to a field (missing counts as 0). Keeps a field TTL.

        Args:
            key: The hash key.
            field: Field name.
            amount: Integer to add (default 1); may be negative.

        Returns:
            The new value (``int``).

        Raises:
            ResponseError: The field is not an integer or the result overflows.
            WrongTypeError: The key holds a non-hash type.
        """
        return self._c(None, "HINCRBY", key, field, amount)

    def hincrbyfloat(self, key: KeyT, field: Arg, amount: float) -> Any:
        """``HINCRBYFLOAT key field amount`` - adds a float to a field (15 significant digits). Keeps a field TTL.

        Args:
            key: The hash key.
            field: Field name.
            amount: Number to add.

        Returns:
            The new value as ``float``.

        Raises:
            ResponseError: The field is not a valid float or the result is not finite.
        """
        return self._c(float, "HINCRBYFLOAT", key, field, amount)

    def hsetnx(self, key: KeyT, field: Arg, value: Arg) -> Any:
        """``HSETNX key field value`` - sets a field only when it does not exist.

        Args:
            key: The hash key.
            field: Field name.
            value: Value.

        Returns:
            ``True`` when set, ``False`` when the field existed (nothing written).
        """
        return self._c(_bool, "HSETNX", key, field, value)

    def hstrlen(self, key: KeyT, field: Arg) -> Any:
        """``HSTRLEN key field`` - length of a field value in bytes.

        Args:
            key: The hash key.
            field: Field name.

        Returns:
            The length; ``0`` when the key or field is missing.
        """
        return self._c(None, "HSTRLEN", key, field)

    def hscan(self, key: KeyT, cursor: int = 0, match: Optional[str] = None, count: Optional[int] = None) -> Any:
        """``HSCAN key cursor [MATCH pattern] [COUNT n]`` - one page of a hash.

        See :meth:`hscan_iter` for the loop. A field can repeat if the table was rebuilt between pages.

        Args:
            key: The hash key.
            cursor: ``0`` to start.
            match: Glob pattern for field names.
            count: Work hint per page.

        Returns:
            ``(next_cursor, {field: value})``; a missing key gives ``(0, {})``.
        """
        args: List[Any] = [key, cursor]
        if match is not None:
            args += ["MATCH", match]
        if count is not None:
            args += ["COUNT", count]
        return self._c(_scan_pairs, "HSCAN", *args)

    def hrandfield(self, key: KeyT, count: Optional[int] = None, withvalues: bool = False) -> Any:
        """``HRANDFIELD key [count [WITHVALUES]]`` - random fields (reads the whole hash, O(N)).

        Args:
            key: The hash key.
            count: Omit for one field. Positive: up to ``count`` distinct fields. Negative: ``-count``
                fields that may repeat (limited to 1,048,576).
            withvalues: With ``count``, return ``(field, value)`` pairs.

        Returns:
            Without ``count``: a field or ``None`` when missing. With ``count``: a list of fields, or of
            ``(field, value)`` tuples with ``withvalues``.
        """
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
        """``HEXPIRE key seconds [NX|XX|GT|LT] FIELDS n field...`` - per-field TTL in seconds.

        Field TTLs follow Redis 7.4. ``HSET`` and ``HDEL`` drop a field TTL; ``HINCRBY`` keeps it. A hash
        whose every field is expired counts as a missing key.

        Args:
            key: The hash key.
            seconds: Time to live; zero or past deletes the field.
            *fields: Field names (at least one).
            nx: Only set when the field has no TTL.
            xx: Only set when the field already has a TTL.
            gt: Only set when the new TTL is greater.
            lt: Only set when the new TTL is smaller.

        Returns:
            A list with one code per field: ``-2`` no such field, ``0`` condition not met, ``1`` TTL
            set, ``2`` field deleted (zero/past time).

        Raises:
            ResponseError: Conflicting conditions or invalid time.
        """
        return self._hfield_ttl("HEXPIRE", key, seconds, fields, _expire_cond(nx, xx, gt, lt))

    def hpexpire(
        self, key: KeyT, ms: int, *fields: Arg, nx: bool = False, xx: bool = False, gt: bool = False, lt: bool = False
    ) -> Any:
        """``HPEXPIRE key ms [NX|XX|GT|LT] FIELDS n field...`` - per-field TTL in milliseconds.

        Args:
            key: The hash key.
            ms: Time to live in milliseconds.
            *fields: Field names.
            nx: Only set when the field has no TTL.
            xx: Only set when the field has a TTL.
            gt: Only set when greater than the current TTL.
            lt: Only set when smaller than the current TTL.

        Returns:
            One code per field, as in :meth:`hexpire`.
        """
        return self._hfield_ttl("HPEXPIRE", key, ms, fields, _expire_cond(nx, xx, gt, lt))

    def hexpireat(
        self, key: KeyT, when: int, *fields: Arg, nx: bool = False, xx: bool = False, gt: bool = False, lt: bool = False
    ) -> Any:
        """``HEXPIREAT key when [NX|XX|GT|LT] FIELDS n field...`` - per-field expiry at a Unix time in seconds.

        Args:
            key: The hash key.
            when: Unix time in seconds; a past time deletes the field.
            *fields: Field names.
            nx: Only set when the field has no TTL.
            xx: Only set when the field has a TTL.
            gt: Only set when later than the current expiry.
            lt: Only set when earlier than the current expiry.

        Returns:
            One code per field, as in :meth:`hexpire`.
        """
        return self._hfield_ttl("HEXPIREAT", key, when, fields, _expire_cond(nx, xx, gt, lt))

    def hpexpireat(
        self, key: KeyT, when_ms: int, *fields: Arg, nx: bool = False, xx: bool = False, gt: bool = False, lt: bool = False
    ) -> Any:
        """``HPEXPIREAT key when_ms [NX|XX|GT|LT] FIELDS n field...`` - per-field expiry at a Unix time in ms.

        Args:
            key: The hash key.
            when_ms: Unix time in milliseconds; a past time deletes the field.
            *fields: Field names.
            nx: Only set when the field has no TTL.
            xx: Only set when the field has a TTL.
            gt: Only set when later than the current expiry.
            lt: Only set when earlier than the current expiry.

        Returns:
            One code per field, as in :meth:`hexpire`.
        """
        return self._hfield_ttl("HPEXPIREAT", key, when_ms, fields, _expire_cond(nx, xx, gt, lt))

    def httl(self, key: KeyT, *fields: Arg) -> Any:
        """``HTTL key FIELDS n field...`` - remaining field TTL in seconds (rounded up).

        Args:
            key: The hash key.
            *fields: Field names.

        Returns:
            A list per field: seconds left, ``-1`` field has no TTL, ``-2`` field or key missing.
        """
        return self._c(None, "HTTL", key, "FIELDS", len(fields), *fields)

    def hpttl(self, key: KeyT, *fields: Arg) -> Any:
        """``HPTTL key FIELDS n field...`` - remaining field TTL in milliseconds.

        Args:
            key: The hash key.
            *fields: Field names.

        Returns:
            A list per field: ms left, ``-1`` no TTL, ``-2`` field or key missing.
        """
        return self._c(None, "HPTTL", key, "FIELDS", len(fields), *fields)

    def hexpiretime(self, key: KeyT, *fields: Arg) -> Any:
        """``HEXPIRETIME key FIELDS n field...`` - field expiry as Unix seconds (rounded up).

        Args:
            key: The hash key.
            *fields: Field names.

        Returns:
            A list per field: Unix time, ``-1`` no TTL, ``-2`` missing.
        """
        return self._c(None, "HEXPIRETIME", key, "FIELDS", len(fields), *fields)

    def hpexpiretime(self, key: KeyT, *fields: Arg) -> Any:
        """``HPEXPIRETIME key FIELDS n field...`` - field expiry as Unix milliseconds.

        Args:
            key: The hash key.
            *fields: Field names.

        Returns:
            A list per field: Unix time in ms, ``-1`` no TTL, ``-2`` missing.
        """
        return self._c(None, "HPEXPIRETIME", key, "FIELDS", len(fields), *fields)

    def hpersist(self, key: KeyT, *fields: Arg) -> Any:
        """``HPERSIST key FIELDS n field...`` - removes field TTLs.

        Args:
            key: The hash key.
            *fields: Field names.

        Returns:
            A list per field: ``1`` TTL removed, ``-1`` field had none, ``-2`` field or key missing.
        """
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
        """``ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]`` - adds or updates sorted-set members.

        Args:
            key: The sorted-set key (created when missing).
            mapping: ``{member: score}`` or an iterable of ``(member, score)`` pairs. Scores are floats;
                ``inf``/``-inf`` are accepted.
            nx: Only add new members, never update existing ones.
            xx: Only update existing members, never add.
            gt: Update only when the new score is greater (new members are still added).
            lt: Update only when the new score is lower (new members are still added).
            ch: Count changed members (new plus updated) instead of only new ones.
            incr: Treat the single pair as an increment, like :meth:`zincrby`.

        Returns:
            The number of new members (or changed ones with ``ch``). With ``incr``: the new score as
            ``float``, or ``None`` when a condition refused the update.

        Raises:
            WrongTypeError: The key holds a non-sorted-set type.
            ResponseError: Conflicting options or a score that is not a number.

        Example:
            client.zadd("board", {"ann": 10, "bob": 7})   # -> 2
            client.zadd("board", {"ann": 12}, ch=True)    # -> 1
        """
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
        """``ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]`` - members in a range.

        By default ``start``/``stop`` are zero-based ranks (negative counts from the end). With
        ``byscore`` they are score bounds (``-inf``, ``+inf``, ``(5`` for exclusive); with ``bylex``
        they are lex bounds (``-``, ``+``, ``[member``, ``(member``). With ``rev`` the order is
        descending and, for ``byscore``/``bylex``, ``start`` is the high bound.

        Args:
            key: The sorted-set key.
            start: Range start (rank, score or lex bound).
            stop: Range end.
            byscore: Interpret the bounds as scores.
            bylex: Interpret the bounds as lexicographic member bounds (scores must be equal).
            rev: Reverse the order.
            offset: Skip this many results (needs ``count``; only with ``byscore``/``bylex``).
            count: Return at most this many (negative returns the rest).
            withscores: Return scores too.

        Returns:
            A list of members, or a list of ``(member, score)`` tuples with ``withscores``. Empty when
            the key is missing.

        Raises:
            ValueError: Only one of ``offset`` and ``count`` was given.
            WrongTypeError: The key holds a non-sorted-set type.

        Example:
            client.zrange("board", 0, -1, withscores=True)   # -> [("bob", 7.0), ("ann", 12.0)]
            client.zrange("board", "(5", "+inf", byscore=True)
        """
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
        """``ZREVRANGE key start stop [WITHSCORES]`` - members by rank, highest score first.

        Args:
            key: The sorted-set key.
            start: First rank (``0`` is the highest score); negative counts from the end.
            stop: Last rank, inclusive.
            withscores: Return ``(member, score)`` tuples.

        Returns:
            A list of members or tuples; empty when the key is missing.
        """
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
        """``ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]`` - members with scores in a range, lowest first.

        Args:
            key: The sorted-set key.
            min: Lower bound: a number, ``"-inf"`` or ``"(5"`` for exclusive.
            max: Upper bound: a number, ``"+inf"`` or ``"(9"`` for exclusive.
            offset: Skip this many (needs ``count``).
            count: Return at most this many (negative returns the rest).
            withscores: Return ``(member, score)`` tuples.

        Returns:
            A list of members or tuples.

        Raises:
            ValueError: Only one of ``offset`` and ``count`` was given.
        """
        args: List[Any] = [key, min, max]
        if withscores:
            args.append("WITHSCORES")
        _limit_args(args, offset, count)
        return self._c(_scored if withscores else None, "ZRANGEBYSCORE", *args)

    def zrangebylex(
        self, key: KeyT, min: Arg, max: Arg, offset: Optional[int] = None, count: Optional[int] = None  # noqa: A002
    ) -> Any:
        """``ZRANGEBYLEX key min max [LIMIT offset count]`` - members in a lexicographic range (equal scores).

        Args:
            key: The sorted-set key.
            min: Lower bound: ``"-"``, ``"[member"`` (inclusive) or ``"(member"`` (exclusive).
            max: Upper bound: ``"+"``, ``"[member"`` or ``"(member"``.
            offset: Skip this many (needs ``count``).
            count: Return at most this many.

        Returns:
            A list of members, ascending.

        Raises:
            ValueError: Only one of ``offset`` and ``count`` was given.
        """
        args: List[Any] = [key, min, max]
        _limit_args(args, offset, count)
        return self._c(None, "ZRANGEBYLEX", *args)

    def zrevrangebylex(
        self, key: KeyT, max: Arg, min: Arg, offset: Optional[int] = None, count: Optional[int] = None  # noqa: A002
    ) -> Any:
        """``ZREVRANGEBYLEX key max min [LIMIT offset count]`` - lexicographic range, descending.

        Args:
            key: The sorted-set key.
            max: High bound (first!): ``"+"``, ``"[member"`` or ``"(member"``.
            min: Low bound: ``"-"``, ``"[member"`` or ``"(member"``.
            offset: Skip this many (needs ``count``).
            count: Return at most this many.

        Returns:
            A list of members, descending.

        Raises:
            ValueError: Only one of ``offset`` and ``count`` was given.
        """
        args: List[Any] = [key, max, min]
        _limit_args(args, offset, count)
        return self._c(None, "ZREVRANGEBYLEX", *args)

    def zrem(self, key: KeyT, *members: Arg) -> Any:
        """``ZREM key member [member ...]`` - removes members.

        Args:
            key: The sorted-set key.
            *members: One or more members.

        Returns:
            The number removed. The key disappears with its last member.
        """
        return self._c(None, "ZREM", key, *members)

    def zincrby(self, key: KeyT, amount: float, member: Arg) -> Any:
        """``ZINCRBY key amount member`` - adds to a member's score (member created at ``amount`` if new).

        Args:
            key: The sorted-set key.
            amount: Number to add; may be negative.
            member: The member.

        Returns:
            The new score as ``float``.
        """
        return self._c(float, "ZINCRBY", key, amount, member)

    def zcard(self, key: KeyT) -> Any:
        """``ZCARD key`` - number of members.

        Args:
            key: The sorted-set key.

        Returns:
            The count; ``0`` when the key is missing.
        """
        return self._c(None, "ZCARD", key)

    def zscore(self, key: KeyT, member: Arg) -> Any:
        """``ZSCORE key member`` - a member's score.

        Args:
            key: The sorted-set key.
            member: The member.

        Returns:
            The score as ``float``, or ``None`` when the member or key is missing.
        """
        return self._c(_float, "ZSCORE", key, member)

    def zmscore(self, key: KeyT, *members: Arg) -> Any:
        """``ZMSCORE key member [member ...]`` - scores of several members.

        Args:
            key: The sorted-set key.
            *members: Members.

        Returns:
            A list of ``float`` or ``None`` per member.
        """
        return self._c(_floats, "ZMSCORE", key, *members)

    def zrank(self, key: KeyT, member: Arg) -> Any:
        """``ZRANK key member`` - zero-based rank, lowest score first.

        Args:
            key: The sorted-set key.
            member: The member.

        Returns:
            The rank (``int``), or ``None`` when the member or key is missing.
        """
        return self._c(None, "ZRANK", key, member)

    def zrevrank(self, key: KeyT, member: Arg) -> Any:
        """``ZREVRANK key member`` - zero-based rank, highest score first.

        Args:
            key: The sorted-set key.
            member: The member.

        Returns:
            The rank (``int``), or ``None`` when missing.
        """
        return self._c(None, "ZREVRANK", key, member)

    def zcount(self, key: KeyT, min: Arg, max: Arg) -> Any:  # noqa: A002
        """``ZCOUNT key min max`` - number of members with a score in a range.

        Args:
            key: The sorted-set key.
            min: Lower bound: number, ``"-inf"`` or ``"(5"`` (exclusive).
            max: Upper bound: number, ``"+inf"`` or ``"(9"`` (exclusive).

        Returns:
            The count (``0`` when the key is missing).
        """
        return self._c(None, "ZCOUNT", key, min, max)

    def zlexcount(self, key: KeyT, min: Arg, max: Arg) -> Any:  # noqa: A002
        """``ZLEXCOUNT key min max`` - number of members in a lexicographic range.

        Args:
            key: The sorted-set key.
            min: ``"-"``, ``"[member"`` or ``"(member"``.
            max: ``"+"``, ``"[member"`` or ``"(member"``.

        Returns:
            The count.
        """
        return self._c(None, "ZLEXCOUNT", key, min, max)

    def zpopmin(self, key: KeyT, count: Optional[int] = None) -> Any:
        """``ZPOPMIN key [count]`` - removes and returns the lowest-scored members.

        Args:
            key: The sorted-set key.
            count: How many to pop (default one).

        Returns:
            A list of ``(member, score)`` tuples, lowest first; empty when the key is missing.
        """
        return self._c(_scored, "ZPOPMIN", key) if count is None else self._c(_scored, "ZPOPMIN", key, count)

    def zpopmax(self, key: KeyT, count: Optional[int] = None) -> Any:
        """``ZPOPMAX key [count]`` - removes and returns the highest-scored members.

        Args:
            key: The sorted-set key.
            count: How many to pop (default one).

        Returns:
            A list of ``(member, score)`` tuples, highest first; empty when the key is missing.
        """
        return self._c(_scored, "ZPOPMAX", key) if count is None else self._c(_scored, "ZPOPMAX", key, count)

    def bzpopmin(self, keys: Union[KeyT, Sequence[KeyT]], timeout: float = 0) -> Any:
        """``BZPOPMIN key [key ...] timeout`` - blocking :meth:`zpopmin` over several keys.

        Blocks the connection (asyncio: use :meth:`dedicated`); at most 1,024 blocked clients per shard;
        keys must share a shard.

        Args:
            keys: One key or a list of keys, checked in order.
            timeout: Seconds to wait (float); ``0`` waits until data arrives.

        Returns:
            ``(key, member, score)``, or ``None`` on timeout.
        """
        return self._c(_bzpop, "BZPOPMIN", *_keys(keys), timeout)

    def bzpopmax(self, keys: Union[KeyT, Sequence[KeyT]], timeout: float = 0) -> Any:
        """``BZPOPMAX key [key ...] timeout`` - blocking :meth:`zpopmax` over several keys.

        Same blocking rules as :meth:`bzpopmin`.

        Args:
            keys: One key or a list of keys.
            timeout: Seconds to wait (float); ``0`` waits until data arrives.

        Returns:
            ``(key, member, score)``, or ``None`` on timeout.
        """
        return self._c(_bzpop, "BZPOPMAX", *_keys(keys), timeout)

    def zremrangebyrank(self, key: KeyT, start: int, stop: int) -> Any:
        """``ZREMRANGEBYRANK key start stop`` - removes members by rank.

        Args:
            key: The sorted-set key.
            start: First rank (negative counts from the end).
            stop: Last rank, inclusive.

        Returns:
            The number removed.
        """
        return self._c(None, "ZREMRANGEBYRANK", key, start, stop)

    def zremrangebyscore(self, key: KeyT, min: Arg, max: Arg) -> Any:  # noqa: A002
        """``ZREMRANGEBYSCORE key min max`` - removes members with a score in a range.

        Args:
            key: The sorted-set key.
            min: Lower bound: number, ``"-inf"`` or ``"(5"`` (exclusive).
            max: Upper bound: number, ``"+inf"`` or ``"(9"`` (exclusive).

        Returns:
            The number removed.
        """
        return self._c(None, "ZREMRANGEBYSCORE", key, min, max)

    def zremrangebylex(self, key: KeyT, min: Arg, max: Arg) -> Any:  # noqa: A002
        """``ZREMRANGEBYLEX key min max`` - removes members in a lexicographic range.

        Args:
            key: The sorted-set key.
            min: ``"-"``, ``"[member"`` or ``"(member"``.
            max: ``"+"``, ``"[member"`` or ``"(member"``.

        Returns:
            The number removed.
        """
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
        """``ZUNIONSTORE dst numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]`` - stores a union.

        Inputs may be sorted sets or plain sets (every member scores 1). An empty result removes ``dst``.

        Args:
            dst: Destination key (overwritten).
            keys: Source keys (same slot on a cluster).
            weights: One multiplier per source key.
            aggregate: How to combine equal members: ``"SUM"`` (default), ``"MIN"`` or ``"MAX"``.

        Returns:
            The number of members stored in ``dst``.

        Raises:
            CrossSlotError: Cluster client - the keys are in different slots.
        """
        return self._zcombine("ZUNIONSTORE", dst, keys, weights, aggregate, False)

    def zinterstore(
        self, dst: KeyT, keys: Sequence[KeyT], weights: Optional[Sequence[float]] = None, aggregate: Optional[str] = None
    ) -> Any:
        """``ZINTERSTORE dst numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]`` - stores an intersection.

        Inputs may be sorted sets or plain sets (score 1). An empty result removes ``dst``.

        Args:
            dst: Destination key (overwritten).
            keys: Source keys (same slot on a cluster).
            weights: One multiplier per source key.
            aggregate: ``"SUM"`` (default), ``"MIN"`` or ``"MAX"``.

        Returns:
            The number of members stored.

        Raises:
            CrossSlotError: Cluster client - the keys are in different slots.
        """
        return self._zcombine("ZINTERSTORE", dst, keys, weights, aggregate, False)

    def zdiffstore(self, dst: KeyT, keys: Sequence[KeyT]) -> Any:
        """``ZDIFFSTORE dst numkeys key [key ...]`` - stores members of the first set absent from the others.

        Args:
            dst: Destination key (overwritten; removed if the result is empty).
            keys: Source keys, the first is the base (same slot on a cluster).

        Returns:
            The number of members stored.
        """
        return self._zcombine("ZDIFFSTORE", dst, keys, None, None, False)

    def zunion(
        self,
        keys: Sequence[KeyT],
        weights: Optional[Sequence[float]] = None,
        aggregate: Optional[str] = None,
        withscores: bool = False,
    ) -> Any:
        """``ZUNION numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...] [WITHSCORES]`` - union without storing.

        Args:
            keys: Source keys (same slot on a cluster).
            weights: One multiplier per source key.
            aggregate: ``"SUM"`` (default), ``"MIN"`` or ``"MAX"``.
            withscores: Return ``(member, score)`` tuples.

        Returns:
            A list of members, or of tuples with ``withscores``.
        """
        return self._zcombine("ZUNION", None, keys, weights, aggregate, withscores)

    def zinter(
        self,
        keys: Sequence[KeyT],
        weights: Optional[Sequence[float]] = None,
        aggregate: Optional[str] = None,
        withscores: bool = False,
    ) -> Any:
        """``ZINTER numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...] [WITHSCORES]`` - intersection without storing.

        Args:
            keys: Source keys (same slot on a cluster).
            weights: One multiplier per source key.
            aggregate: ``"SUM"`` (default), ``"MIN"`` or ``"MAX"``.
            withscores: Return ``(member, score)`` tuples.

        Returns:
            A list of members, or of tuples with ``withscores``.
        """
        return self._zcombine("ZINTER", None, keys, weights, aggregate, withscores)

    def zdiff(self, keys: Sequence[KeyT], withscores: bool = False) -> Any:
        """``ZDIFF numkeys key [key ...] [WITHSCORES]`` - members of the first set absent from the others.

        Args:
            keys: Source keys, the first is the base (same slot on a cluster).
            withscores: Return ``(member, score)`` tuples.

        Returns:
            A list of members, or of tuples with ``withscores``.
        """
        return self._zcombine("ZDIFF", None, keys, None, None, withscores)

    def zrandmember(self, key: KeyT, count: Optional[int] = None, withscores: bool = False) -> Any:
        """``ZRANDMEMBER key [count [WITHSCORES]]`` - random members (reads the whole set, O(N)).

        Args:
            key: The sorted-set key.
            count: Omit for one member. Positive: distinct members. Negative: ``-count`` members that
                may repeat (limited to 1,048,576).
            withscores: With ``count``, return ``(member, score)`` tuples.

        Returns:
            Without ``count``: a member or ``None``. With ``count``: a list.
        """
        if count is None:
            return self._c(None, "ZRANDMEMBER", key)
        if withscores:
            return self._c(_scored, "ZRANDMEMBER", key, count, "WITHSCORES")
        return self._c(None, "ZRANDMEMBER", key, count)

    def zscan(self, key: KeyT, cursor: int = 0, match: Optional[str] = None, count: Optional[int] = None) -> Any:
        """``ZSCAN key cursor [MATCH pattern] [COUNT n]`` - one page of members with scores, in score order.

        See :meth:`zscan_iter`. Pages are by position, so a concurrent write can shift members between pages.

        Args:
            key: The sorted-set key.
            cursor: ``0`` to start.
            match: Glob pattern for members.
            count: Work hint per page.

        Returns:
            ``(next_cursor, [(member, score), ...])``; a missing key gives ``(0, [])``.
        """
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
        """``GEOADD key [NX|XX] [CH] longitude latitude member [...]`` - adds places to a geo index.

        A geo index is a sorted set whose score is a 52-bit geohash, so ``type`` reports ``zset`` and the
        ``z*`` commands work on it. Coordinates are stored with about 0.6 m precision, so
        :meth:`geopos` can differ slightly from what you added.

        Args:
            key: The geo key (created when missing).
            values: Iterable of ``(longitude, latitude, member)`` tuples. Longitude must be in
                -180..180 and latitude in -85.05112878..85.05112878 (decimal degrees).
            nx: Only add new members.
            xx: Only update existing members.
            ch: Count changed members instead of only new ones.

        Returns:
            The number of members added (or changed with ``ch``).

        Raises:
            ResponseError: A coordinate is out of range (``invalid longitude,latitude pair``).
            WrongTypeError: The key holds a non-sorted-set type.

        Example:
            client.geoadd("shops", [(13.361389, 38.115556, "palermo"), (15.087269, 37.502669, "catania")])
        """
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
        """``GEODIST key member1 member2 [unit]`` - distance between two members.

        Args:
            key: The geo key.
            member1: First member.
            member2: Second member.
            unit: ``"m"`` (default), ``"km"``, ``"ft"`` or ``"mi"``.

        Returns:
            The distance as ``float`` in ``unit``, or ``None`` when a member or the key is missing.

        Raises:
            ResponseError: Unsupported unit.

        Example:
            client.geodist("shops", "palermo", "catania", "km")   # -> 166.27
        """
        if unit is None:
            return self._c(_float, "GEODIST", key, member1, member2)
        return self._c(_float, "GEODIST", key, member1, member2, unit)

    def geohash(self, key: KeyT, *members: Arg) -> Any:
        """``GEOHASH key member [member ...]`` - standard geohash strings.

        Args:
            key: The geo key.
            *members: One or more members.

        Returns:
            A list of 11-character strings, ``None`` for a missing member.
        """
        return self._c(None, "GEOHASH", key, *members)

    def geopos(self, key: KeyT, *members: Arg) -> Any:
        """``GEOPOS key member [member ...]`` - coordinates of members.

        Args:
            key: The geo key.
            *members: One or more members.

        Returns:
            A list of ``(longitude, latitude)`` float tuples, ``None`` for a missing member.
        """
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
        """``GEOSEARCH key FROMMEMBER m|FROMLONLAT lon lat BYRADIUS r unit|BYBOX w h unit [ASC|DESC] [COUNT n [ANY]] [WITH...]`` - finds members in an area.

        Give exactly one center (``member`` or ``longitude`` + ``latitude``) and exactly one shape
        (``radius`` or ``width`` + ``height``).

        Args:
            key: The geo key.
            member: Center on this existing member.
            longitude: Center longitude (-180..180); use with ``latitude``.
            latitude: Center latitude (-85.05112878..85.05112878).
            radius: Circle radius, in ``unit`` (non-negative).
            width: Box width, in ``unit`` (non-negative); use with ``height``.
            height: Box height, in ``unit``.
            unit: ``"m"`` (default), ``"km"``, ``"ft"`` or ``"mi"``; also the unit of returned distances.
            sort: ``"ASC"`` (nearest first) or ``"DESC"``. Unsorted when omitted and no ``count``.
            count: Return at most this many (positive). Without ``any`` the nearest are returned.
            any: Stop at the first ``count`` matches (faster, not the nearest). Needs ``count``.
            withcoord: Include coordinates.
            withdist: Include the distance from the center.
            withhash: Include the raw 52-bit geohash integer.

        Returns:
            A list of members. If any ``with*`` flag is set: a list of :class:`GeoResult`
            ``(member, dist, hash, coord)`` where unrequested parts are ``None``. Empty when the key is missing.

        Raises:
            ResponseError: Missing center or shape, both given, ``any`` without ``count``, bad unit, or
                a center ``member`` that does not exist.

        Example:
            client.geosearch("shops", longitude=15, latitude=37, radius=200, unit="km",
                             sort="ASC", withdist=True)
        """
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
        """``GEOSEARCHSTORE dst src FROM... BY... [ASC|DESC] [COUNT n [ANY]] [STOREDIST]`` - like :meth:`geosearch`, stores the result.

        The destination is replaced with a geo sorted set of the matches.

        Args:
            dst: Destination key.
            src: Source geo key.
            member: Center on this member.
            longitude: Center longitude; use with ``latitude``.
            latitude: Center latitude.
            radius: Circle radius in ``unit``.
            width: Box width in ``unit``; use with ``height``.
            height: Box height in ``unit``.
            unit: ``"m"`` (default), ``"km"``, ``"ft"`` or ``"mi"``.
            sort: ``"ASC"`` or ``"DESC"``.
            count: Keep at most this many.
            any: Stop at the first ``count`` matches. Needs ``count``.
            storedist: Store the distance (in ``unit``) as the score instead of the geohash.

        Returns:
            The number of members stored.

        Raises:
            CrossSlotError: Cluster client - ``dst`` and ``src`` are in different slots.
        """
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
        """``GEORADIUS key longitude latitude radius unit [WITH...] [COUNT n [ANY]] [ASC|DESC] [STORE k] [STOREDIST k]`` - legacy radius search; prefer :meth:`geosearch`.

        Args:
            key: The geo key.
            longitude: Center longitude (-180..180).
            latitude: Center latitude (-85.05112878..85.05112878).
            radius: Radius in ``unit`` (non-negative).
            unit: ``"m"`` (default), ``"km"``, ``"ft"`` or ``"mi"``.
            withcoord: Include coordinates.
            withdist: Include distance from the center.
            withhash: Include the geohash integer.
            count: Return at most this many.
            any: Stop at the first ``count`` matches (needs ``count``).
            sort: ``"ASC"`` or ``"DESC"``.
            store: Store matching members (geohash scores) in this key instead of returning them.
            storedist: Store matching members with their distance as score in this key.

        Returns:
            Members, or :class:`GeoResult` rows when a ``with*`` flag is set. With ``store`` or
            ``storedist``: the number of stored members. ``with*`` cannot be combined with storing.

        Raises:
            CrossSlotError: Cluster client - the store key is in another slot.
        """
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
        """``GEORADIUSBYMEMBER key member radius unit [...]`` - like :meth:`georadius` centered on a member.

        Args:
            key: The geo key.
            member: Existing member to center on.
            radius: Radius in ``unit`` (non-negative).
            unit: ``"m"`` (default), ``"km"``, ``"ft"`` or ``"mi"``.
            withcoord: Include coordinates.
            withdist: Include distance from the member.
            withhash: Include the geohash integer.
            count: Return at most this many.
            any: Stop at the first ``count`` matches (needs ``count``).
            sort: ``"ASC"`` or ``"DESC"``.
            store: Store matches in this key instead of returning them.
            storedist: Store matches with distance as score in this key.

        Returns:
            Members, :class:`GeoResult` rows, or the number stored; see :meth:`georadius`.

        Raises:
            ResponseError: ``member`` does not exist.
        """
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
