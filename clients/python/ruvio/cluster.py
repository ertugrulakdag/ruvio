# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Synchronous cluster client: ``Ruvio(host, port, cluster=True)``.

On the first command (or ``connect()``) the seed is asked for ``CLUSTER SLOTS``. A server that
answers ``-ERR cluster mode is disabled`` (or reports a single node) is used as a plain standalone
server from then on. Otherwise a 16384-entry slot -> node table is built and every command is sent
to the node that owns its key, over one lazily opened socket per node.

Only this class pays for routing: ``Ruvio(...)`` without ``cluster=True`` is untouched.
"""

from __future__ import annotations

import socket
from typing import Any, Dict, List, Optional, Tuple

from . import _cluster as cl
from ._commands import Conv
from ._pipeline import _EXEC, _MULTI
from .client import PubSub, Ruvio, _Conn, _Options
from .errors import ConnectionError, CrossSlotError, MovedError, ResponseError, RuvioError, TimeoutError

__all__ = ["ClusterRuvio"]


class _Node:
    __slots__ = ("host", "port", "conn", "idx")

    def __init__(self, host: str, port: int, conn: _Conn) -> None:
        self.host = host
        self.port = port
        self.conn = conn
        self.idx = -1


class _State:
    """Routing state, shared by a client and its ``raw`` view."""

    __slots__ = ("mode", "slots", "nodes", "by_addr", "seed", "moved", "watch")

    def __init__(self, seed: _Node) -> None:
        self.mode = 0  # 0 = not discovered yet, 1 = cluster, 2 = standalone fallback
        self.slots: List[int] = []
        self.nodes: List[_Node] = []
        self.by_addr: Dict[Tuple[str, int], _Node] = {(seed.host, seed.port): seed}
        self.seed = seed
        self.moved = 0  # MOVED replies since the last full refresh
        self.watch: Optional[_Node] = None


def _fail(conn: _Conn, exc: BaseException) -> BaseException:
    conn.close()
    if isinstance(exc, socket.timeout):
        return TimeoutError("timed out waiting for the reply")
    if isinstance(exc, ConnectionError):
        return exc
    if isinstance(exc, OSError):
        return ConnectionError(str(exc))
    return exc


class ClusterRuvio(Ruvio):
    """Cluster-aware synchronous client (see the module docstring and README "Cluster").

    * single-key commands go to the owner of the key's slot; keyless ones to the seed;
    * MGET/MSET/DEL/UNLINK/EXISTS/TOUCH over several nodes are split per node, sent to all nodes
      before any reply is read, and re-assembled in the caller's order;
    * any other command whose keys span slots raises :class:`CrossSlotError` before sending;
    * ``-MOVED`` patches/refreshes the slot table and retries the command once;
    * a failed node socket is closed and reopened by the next command for that node; a command
      that hit a transport failure is never replayed;
    * ``scan()`` walks the nodes one after the other (cursor = server_cursor * nodes + node).
    """

    __slots__ = ("_cl",)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        o = self._conn.opts
        if o.unix_socket_path:
            raise ValueError("cluster mode needs host:port, not a unix socket")
        self._cl = _State(_Node(o.host, o.port, self._conn))
        if self._conn.sock is not None:  # lazy=False connected the seed already
            self._discover()

    # ---------------------------------------------------------------- transport
    def _rt(self, conn: _Conn, data: bytes, dec: bool) -> Any:
        """One round trip on ``conn``; returns the raw reply (a ``ResponseError`` is a value)."""
        if conn.sock is None:
            conn.open()
        p = conn.parser
        if p.decode is not dec:
            p.decode = dec
        try:
            conn.sock.sendall(data)  # type: ignore[union-attr]
            return conn.read()
        except BaseException as exc:
            raise _fail(conn, exc) from None

    def _write(self, conn: _Conn, data: bytes, dec: bool) -> None:
        if conn.sock is None:
            conn.open()
        p = conn.parser
        if p.decode is not dec:
            p.decode = dec
        try:
            conn.sock.sendall(data)  # type: ignore[union-attr]
        except BaseException as exc:
            raise _fail(conn, exc) from None

    def _read(self, conn: _Conn, n: int) -> List[Any]:
        try:
            return [conn.read() for _ in range(n)]
        except BaseException as exc:
            raise _fail(conn, exc) from None

    def _scatter(self, jobs: List[Tuple[_Conn, bytes, int]], dec: bool) -> List[Any]:
        """Writes every job to its node first, then reads: the nodes work in parallel.

        Returns one entry per job (a list of raw replies, or the exception for a dead node)."""
        sent: List[Any] = []
        for conn, data, n in jobs:
            try:
                self._write(conn, data, dec)
                sent.append(n)
            except (ConnectionError, OSError) as exc:
                sent.append(exc)
        out: List[Any] = []
        for (conn, _, _), n in zip(jobs, sent):
            if isinstance(n, BaseException):
                out.append(n)
                continue
            try:
                out.append(self._read(conn, n))
            except (ConnectionError, OSError) as exc:
                out.append(exc)
        return out

    # ---------------------------------------------------------------- topology
    def _new_node(self, host: str, port: int) -> _Node:
        o = _Options(**self._conn.opts.as_dict())
        o.host = host
        o.port = port
        return _Node(host, port, _Conn(o))

    def _discover(self) -> None:
        c = self._cl
        reply = self._rt(c.seed.conn, cl.CLUSTER_SLOTS, True)
        topo = cl.parse_slots(reply, c.seed.host) if not isinstance(reply, ResponseError) else None
        if topo is None or len(topo.nodes) < 2:
            c.mode = 2  # standalone (or a one-node map): plain single-connection behavior
            return
        self._apply(topo)
        c.mode = 1

    def _apply(self, topo: cl.Topology) -> None:
        c = self._cl
        old = c.by_addr
        nodes: List[_Node] = []
        by: Dict[Tuple[str, int], _Node] = {}
        for addr in topo.nodes:
            n = old.get(addr)
            if n is None:
                n = self._new_node(*addr)
            n.idx = len(nodes)
            nodes.append(n)
            by[addr] = n
        if (c.seed.host, c.seed.port) not in by:
            by[(c.seed.host, c.seed.port)] = c.seed
        for addr, n in old.items():
            if addr not in by:
                n.conn.close()
        for n in by.values():
            if n not in nodes:
                n.idx = -1
        c.nodes = nodes
        c.by_addr = by
        c.slots = topo.slots
        c.moved = 0

    def _refresh(self) -> bool:
        c = self._cl
        tried = set()
        for node in [c.seed, *c.nodes]:
            if id(node) in tried:
                continue
            tried.add(id(node))
            try:
                reply = self._rt(node.conn, cl.CLUSTER_SLOTS, True)
            except (ConnectionError, OSError):
                continue
            topo = None if isinstance(reply, ResponseError) else cl.parse_slots(reply, c.seed.host)
            if topo is not None and len(topo.nodes) >= 1:
                self._apply(topo)
                return True
        return False

    def refresh_slots(self) -> None:
        """Re-reads ``CLUSTER SLOTS`` and rebuilds the slot table."""
        if self._cl.mode == 0:
            self._discover()
        elif self._cl.mode == 1:
            self._refresh()

    @property
    def is_cluster(self) -> bool:
        """True when the seed answered ``CLUSTER SLOTS`` with a multi-node map."""
        if self._cl.mode == 0:
            self._discover()
        return self._cl.mode == 1

    @property
    def nodes(self) -> List[Tuple[str, int]]:
        """``(host, port)`` of the known nodes in slot order (empty in standalone fallback)."""
        if self._cl.mode == 0:
            self._discover()
        return [(n.host, n.port) for n in self._cl.nodes]

    def _owner(self, slot: Optional[int]) -> _Node:
        c = self._cl
        if slot is None:
            return c.seed
        i = c.slots[slot]
        return c.nodes[i] if i >= 0 else c.seed

    # ---------------------------------------------------------------- routing
    def _x(self, conv: Conv, data: bytes, decode: Optional[bool] = None) -> Any:
        c = self._cl
        if c.mode != 1:
            if c.mode == 0:
                self._discover()
            if c.mode == 2:
                return Ruvio._x(self, conv, data, decode)
        dec = self._decode if decode is None else decode
        name, key = cl.frame_head(data)
        if not name.isupper():
            name = name.upper()
        if name not in cl.SPEC:
            # the common case: one key, argument 1 (the slot table lookup is two list indexes)
            if key is None:
                node = c.seed
            else:
                i = c.slots[cl.key_slot(key)]
                node = c.nodes[i] if i >= 0 else c.seed
            reply = self._rt(node.conn, data, dec)
            if reply.__class__ is MovedError:
                reply = self._redirect(reply, data, dec)
        else:
            reply = self._special(name, data, dec)
        if isinstance(reply, ResponseError):
            raise reply
        return reply if conv is None else conv(reply)

    def _redirect(self, err: MovedError, data: bytes, dec: bool) -> Any:
        """``-MOVED``: patch one slot, or refresh the table when more than one slot moved or the
        node is unknown, then retry the command once on the node named by the reply."""
        mv = cl.parse_moved(err)
        if mv is None:
            return err
        slot, host, port = mv
        c = self._cl
        c.moved += 1
        node = c.by_addr.get((host, port))
        if node is None or node.idx < 0 or c.moved > 1:
            self._refresh()
            node = c.by_addr.get((host, port))
            if node is None:
                node = self._new_node(host, port)
                c.by_addr[(host, port)] = node
        else:
            c.slots[slot] = node.idx
        return self._rt(node.conn, data, dec)

    def _single(self, slot: Optional[int], data: bytes, dec: bool) -> Any:
        reply = self._rt(self._owner(slot).conn, data, dec)
        if reply.__class__ is MovedError:
            reply = self._redirect(reply, data, dec)
        return reply

    def _special(self, name: bytes, data: bytes, dec: bool) -> Any:
        args = cl.split_frame(data)
        p = cl.plan(name, args)
        kind = p[0]
        c = self._cl
        if kind == cl.PLAN_SLOT:
            if name == b"UNWATCH" and c.watch is not None:
                node, c.watch = c.watch, None
                return self._rt(node.conn, data, dec)
            reply = self._single(p[1], data, dec)
            if name == b"WATCH" and not isinstance(reply, ResponseError):
                c.watch = self._owner(p[1])
            return reply
        if kind == cl.PLAN_SPLIT:
            return self._split(name, args, p[1], dec)
        if kind == cl.PLAN_SCAN:
            return self._scan(args, dec)
        return self._fan(p[1], data, dec)

    def _fan(self, mode: int, data: bytes, dec: bool) -> Any:
        c = self._cl
        jobs = [(n.conn, data, 1) for n in c.nodes]
        results = self._scatter(jobs, dec)
        replies: List[Any] = []
        for r in results:
            if isinstance(r, BaseException):
                raise r
            if isinstance(r[0], ResponseError):
                return r[0]
            replies.append(r[0])
        if mode == cl.FAN_SUM:
            return sum(replies)
        if mode == cl.FAN_CAT:
            out: List[Any] = []
            for r in replies:
                out.extend(r)
            return out
        return replies[0]

    def _split(self, name: bytes, args: List[bytes], keys: List[bytes], dec: bool) -> Any:
        groups = cl.split_groups(name, args, keys, self._owner_by_slot)
        order = list(groups.items())
        results = self._scatter([(node.conn, b"".join(f for _, f in subs), len(subs)) for node, subs in order], dec)
        parts: List[Tuple[List[int], Any]] = []
        failure: Optional[BaseException] = None
        for (node, subs), r in zip(order, results):
            if isinstance(r, BaseException):
                failure = failure or r
                continue
            for (pos, frame), reply in zip(subs, r):
                if reply.__class__ is MovedError:
                    reply = self._redirect(reply, frame, dec)
                parts.append((pos, reply))
        if failure is not None:
            raise failure
        return cl.merge_split(name, len(keys), parts)

    def _owner_by_slot(self, slot: int) -> _Node:
        return self._owner(slot)

    def _scan(self, args: List[bytes], dec: bool) -> Any:
        c = self._cl
        try:
            node_i, frame = cl.scan_prepare(args, len(c.nodes))
        except ResponseError as e:
            return e
        reply = self._rt(c.nodes[node_i].conn, frame, dec)
        if isinstance(reply, ResponseError):
            return reply
        return cl.scan_finish(reply, node_i, len(c.nodes))

    # ---------------------------------------------------------------- pipelines
    def _batch(self, payload: List[bytes], count: int) -> List[Any]:
        c = self._cl
        if c.mode != 1:
            if c.mode == 0:
                self._discover()
            if c.mode == 2:
                return Ruvio._batch(self, payload, count)
        dec = self._decode
        tx = payload[0] is _MULTI and payload[-1] is _EXEC
        if tx:
            return self._batch_tx(payload, dec)
        # group by node, one pipelined write per node, replies back in the caller's order
        groups = cl.pipeline_groups(payload, c.slots)  # raises before anything is sent
        order = [((c.nodes[ni] if ni >= 0 else c.seed), pos, frames) for ni, (pos, frames) in groups.items()]
        results = self._scatter([(node.conn, b"".join(frames), len(frames)) for node, _, frames in order], dec)
        replies: List[Any] = [None] * len(payload)
        failure: Optional[BaseException] = None
        for (node, pos, frames), r in zip(order, results):
            if isinstance(r, BaseException):
                failure = failure or r
                continue
            for i, v in zip(pos, r):
                replies[i] = v
        if failure is not None:
            raise failure
        for i, v in enumerate(replies):
            if v.__class__ is MovedError:
                replies[i] = self._redirect(v, payload[i], dec)
        return replies

    def _batch_tx(self, payload: List[bytes], dec: bool) -> List[Any]:
        c = self._cl
        slot: Optional[int] = None
        for f in payload[1:-1]:
            s = cl.pipeline_slot(f)
            if s is None:
                continue
            if slot is None:
                slot = s
            elif s != slot:
                raise CrossSlotError("CROSSSLOT the commands of a transaction must use keys in one slot (nothing was sent)")
        node = self._owner(slot)
        if c.watch is not None and c.watch is not node:
            c.watch = None
            raise CrossSlotError("CROSSSLOT the watched keys and the transaction keys live on different nodes")
        c.watch = None
        data = b"".join(payload)
        for attempt in (0, 1):
            replies = self._read_after_write(node.conn, data, len(payload), dec)
            moved = next((r for r in replies if r.__class__ is MovedError), None)
            if moved is None or attempt:
                return replies
            # nothing ran (MOVED commands are refused at queue time): redirect the whole transaction once
            mv = cl.parse_moved(moved)
            self._cl.moved = 2
            self._refresh()
            target = c.by_addr.get((mv[1], mv[2])) if mv else None
            node = target or self._owner(mv[0] if mv else slot)
        return replies

    def _read_after_write(self, conn: _Conn, data: bytes, n: int, dec: bool) -> List[Any]:
        self._write(conn, data, dec)
        return self._read(conn, n)

    # ---------------------------------------------------------------- connection-wide state
    def _share_opts(self) -> None:
        o = self._conn.opts
        c = self._cl
        for n in {id(x): x for x in [c.seed, *c.nodes, *c.by_addr.values()]}.values():
            no = n.conn.opts
            no.password, no.username, no.db, no.client_name = o.password, o.username, o.db, o.client_name

    def select(self, db: int) -> Any:
        ok = super().select(db)
        self._share_opts()
        return ok

    def client_setname(self, name: str) -> Any:
        ok = super().client_setname(name)
        self._share_opts()
        return ok

    def auth(self, password: str, username: Optional[str] = None) -> Any:
        ok = super().auth(password, username)
        self._share_opts()
        return ok

    @property
    def raw(self) -> "ClusterRuvio":
        view = ClusterRuvio.__new__(ClusterRuvio)
        view._conn = self._conn
        view._cl = self._cl
        view._decode = False
        return view

    def connect(self) -> "ClusterRuvio":
        if self._conn.sock is None:
            self._conn.open()
        if self._cl.mode == 0:
            self._discover()
        return self

    def close(self) -> None:
        c = self._cl
        for n in {id(x): x for x in [c.seed, *c.nodes, *c.by_addr.values()]}.values():
            n.conn.close()
        c.watch = None

    def dedicated(self) -> "ClusterRuvio":
        """A new cluster client (own sockets) with the same options, e.g. for WATCH or blocking commands."""
        opts = self._conn.opts.as_dict()
        return ClusterRuvio(decode_responses=self._decode, cluster=True, **opts)

    def pubsub(self, ignore_subscribe_messages: bool = False) -> "ClusterPubSub":
        """Pub/Sub on one node. Classic channels and patterns meet on the owner of slot 0 (where
        ``PUBLISH`` goes); ``ssubscribe`` connects to the owner of the channel's slot."""
        if self._cl.mode == 0:
            self._discover()
        return ClusterPubSub(self, ignore_subscribe_messages)


class ClusterPubSub(PubSub):
    def __init__(self, client: ClusterRuvio, ignore_subscribe_messages: bool = False) -> None:
        c = client._cl
        node = client._owner(0) if c.mode == 1 else c.seed
        opts = _Options(**client._conn.opts.as_dict())
        opts.host, opts.port = node.host, node.port
        super().__init__(opts, client._decode, ignore_subscribe_messages)
        self._client = client

    def ssubscribe(self, *channels: Any) -> None:
        c = self._client._cl
        if c.mode == 1 and channels:
            slots = {cl.key_slot(ch) for ch in channels}
            if len(slots) > 1:
                raise CrossSlotError("CROSSSLOT sharded channels of one SSUBSCRIBE must share a slot")
            node = self._client._owner(slots.pop())
            o = self._conn.opts
            if self._conn.sock is None and not self.shard_channels and not self.channels and not self.patterns:
                o.host, o.port = node.host, node.port
            elif (o.host, o.port) != (node.host, node.port):
                raise RuvioError(
                    f"this Pub/Sub session is connected to {o.host}:{o.port} but the channel lives on "
                    f"{node.host}:{node.port}; open a separate pubsub() for it"
                )
        super().ssubscribe(*channels)
