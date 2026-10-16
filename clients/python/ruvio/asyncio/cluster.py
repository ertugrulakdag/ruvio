# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""asyncio cluster client: ``Ruvio(host, port, cluster=True)``.

Same routing rules as the sync :class:`ruvio.cluster.ClusterRuvio`. Every node gets one lazily
opened, multiplexed connection (the same pipelined protocol object as the standalone client), so
many coroutines share each node connection. Commands that park a connection (``BLPOP``,
``XREAD ... BLOCK``, ``FILLGET ... WAIT`` ...) run on a short-lived dedicated connection to the
owning node so they never stall the shared one.
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Dict, List, Optional, Tuple

from .. import _cluster as cl
from .._commands import Conv
from .._pipeline import _EXEC, _MULTI
from ..client import _Options
from ..errors import ConnectionError, CrossSlotError, MovedError, ResponseError, RuvioError
from .client import PubSub, Ruvio, _Batch, _open

__all__ = ["ClusterRuvio"]

_SPEC = cl.SPEC
_key_slot = cl.key_slot
_frame_head = cl.frame_head


class _Node:
    __slots__ = ("host", "port", "opts", "proto", "lock", "idx")

    def __init__(self, host: str, port: int, opts: _Options) -> None:
        self.host = host
        self.port = port
        self.opts = opts
        self.proto: Any = None
        self.lock: Optional[asyncio.Lock] = None
        self.idx = -1


class _State:
    __slots__ = ("mode", "slots", "nodes", "by_addr", "seed", "watch", "gate")

    def __init__(self, seed: _Node) -> None:
        self.mode = 0
        self.slots: List[int] = []
        self.nodes: List[_Node] = []
        self.by_addr: Dict[Tuple[str, int], _Node] = {(seed.host, seed.port): seed}
        self.seed = seed
        self.watch: Optional[_Node] = None
        self.gate: Optional[asyncio.Lock] = None  # serializes discovery / refresh


class _RoutedFuture(asyncio.Future):  # type: ignore[type-arg]
    """The future of a single-key command handed to the shared connection. It behaves like any
    future; only a ``-MOVED`` reply is intercepted (in ``set_exception``) and followed to the node
    the reply names. This keeps the warm path free of an extra coroutine."""

    __slots__ = ("owner", "data", "conv", "dec")

    def set_exception(self, exc: BaseException) -> None:
        if exc.__class__ is MovedError and not self.done():
            asyncio.ensure_future(self.owner._retry_moved(self, exc), loop=self.get_loop())
        else:
            asyncio.Future.set_exception(self, exc)


def _is_blocking(name: bytes, args: List[bytes]) -> bool:
    if name in cl.BLOCKING:
        return True
    if name == b"XREAD" or name == b"XREADGROUP":
        return any(a.upper() == b"BLOCK" for a in args)
    if name == b"FILLGET":
        return any(a.upper() == b"WAIT" for a in args)
    return False


class ClusterRuvio(Ruvio):
    """Cluster-aware asyncio client (see :class:`ruvio.cluster.ClusterRuvio` for the rules).

    The slot table is discovered on the first command or ``await client.connect()``; a standalone
    server is detected there and used as a plain single connection."""

    __slots__ = ("_cl",)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        o = self._st.opts
        if o.unix_socket_path:
            raise ValueError("cluster mode needs host:port, not a unix socket")
        self._cl = _State(_Node(o.host, o.port, o))

    # ---------------------------------------------------------------- connections
    async def _open_node(self, node: _Node) -> Any:
        proto = node.proto
        if proto is not None and not proto.closed:
            return proto
        if node.lock is None:
            node.lock = asyncio.Lock()
        async with node.lock:
            proto = node.proto
            if proto is None or proto.closed:
                proto = await _open(node.opts, node.opts.on_push)

                def lost(p: Any, node: _Node = node) -> None:
                    if node.proto is p:
                        node.proto = None

                proto.on_lost = lost
                node.proto = proto
            return proto

    async def _send(self, node: _Node, data: bytes, conv: Conv, dec: bool) -> Any:
        """One round trip on the node's shared connection. Raises on error replies."""
        proto = node.proto
        if proto is None or proto.closed:
            proto = await self._open_node(node)
        fut = proto.loop.create_future()
        proto.pending.append((fut, conv, dec))
        proto.send(data)
        return await fut

    async def _send_blocking(self, node: _Node, data: bytes, conv: Conv, dec: bool) -> Any:
        proto = await _open(node.opts, None)
        try:
            fut = proto.loop.create_future()
            proto.pending.append((fut, conv, dec))
            proto.transport.write(data)
            return await fut
        finally:
            proto.on_lost = None
            proto.transport.close()

    async def _node_batch(self, node: _Node, frames: List[bytes], dec: bool) -> List[Any]:
        proto = node.proto
        if proto is None or proto.closed:
            proto = await self._open_node(node)
        fut = proto.loop.create_future()
        n = len(frames)
        idle = not proto.pending and proto.wbuf is None
        entry = (_Batch(fut, n), None, dec)
        proto.pending.extend([entry] * n)
        data = frames[0] if n == 1 else b"".join(frames)
        if idle:
            proto.transport.write(data)
        else:
            proto.send(data)
        return await fut

    # ---------------------------------------------------------------- topology
    def _new_node(self, host: str, port: int) -> _Node:
        o = _Options(**self._st.opts.as_dict())
        o.host = host
        o.port = port
        return _Node(host, port, o)

    async def _discover(self) -> None:
        c = self._cl
        if c.gate is None:
            c.gate = asyncio.Lock()
        async with c.gate:
            if c.mode != 0:
                return
            # the seed rides the base client's connection so a standalone fallback keeps using it
            await Ruvio.connect(self)
            try:
                reply = await Ruvio._x(self, None, cl.CLUSTER_SLOTS, True)
            except ResponseError:
                c.mode = 2
                return
            topo = cl.parse_slots(reply, c.seed.host)
            if topo is None or len(topo.nodes) < 2:
                c.mode = 2
                return
            proto = self._st.proto
            if proto is not None:
                self._adopt_seed(proto)
            self._apply(topo)
            c.mode = 1

    def _adopt_seed(self, proto: Any) -> None:
        seed = self._cl.seed
        seed.proto = proto
        orig = proto.on_lost

        def lost(p: Any, orig: Any = orig, seed: _Node = seed) -> None:
            if orig is not None:
                orig(p)
            if seed.proto is p:
                seed.proto = None

        proto.on_lost = lost

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
            if addr not in by and n is not c.seed and n.proto is not None:
                n.proto.on_lost = None
                n.proto.transport.close()
                n.proto = None
        for n in by.values():
            if n not in nodes:
                n.idx = -1
        c.nodes = nodes
        c.by_addr = by
        c.slots = topo.slots

    async def _refresh(self) -> bool:
        c = self._cl
        seen = set()
        for node in [c.seed, *c.nodes]:
            if id(node) in seen:
                continue
            seen.add(id(node))
            try:
                reply = await self._send(node, cl.CLUSTER_SLOTS, None, True)
            except (ConnectionError, OSError, ResponseError):
                continue
            topo = cl.parse_slots(reply, c.seed.host)
            if topo is not None:
                self._apply(topo)
                return True
        return False

    async def refresh_slots(self) -> None:
        """Re-reads ``CLUSTER SLOTS`` and rebuilds the slot table."""
        if self._cl.mode == 0:
            await self._discover()
        elif self._cl.mode == 1:
            await self._refresh()

    async def is_cluster(self) -> bool:
        if self._cl.mode == 0:
            await self._discover()
        return self._cl.mode == 1

    async def nodes(self) -> List[Tuple[str, int]]:
        """``(host, port)`` of the known nodes in slot order (empty in standalone fallback)."""
        if self._cl.mode == 0:
            await self._discover()
        return [(n.host, n.port) for n in self._cl.nodes]

    def _owner(self, slot: Optional[int]) -> _Node:
        c = self._cl
        if slot is None:
            return c.seed
        i = c.slots[slot]
        return c.nodes[i] if i >= 0 else c.seed

    # ---------------------------------------------------------------- routing
    def _x(self, conv: Conv, data: bytes, decode: Optional[bool] = None) -> Awaitable[Any]:
        c = self._cl
        if c.mode == 2:  # standalone fallback: exactly the plain client's path
            return Ruvio._x(self, conv, data, decode)
        if c.mode == 1:
            name, key = _frame_head(data)
            if not name.isupper():
                name = name.upper()
            if name not in _SPEC and name != b"FILLGET":
                # warm path: no coroutine, the node's pending-reply future is returned directly
                if key is None:
                    node = c.seed
                else:
                    i = c.slots[_key_slot(key)]
                    node = c.nodes[i] if i >= 0 else c.seed
                proto = node.proto
                if proto is not None and not proto.closed:
                    dec = self._decode if decode is None else decode
                    fut = _RoutedFuture(loop=proto.loop)
                    fut.owner = self
                    fut.data = data
                    fut.conv = conv
                    fut.dec = dec
                    proto.pending.append((fut, conv, dec))
                    proto.send(data)
                    return fut
        return self._route(conv, data, decode)

    async def _retry_moved(self, fut: "_RoutedFuture", err: MovedError) -> None:
        try:
            reply = await self._redirect(err, fut.data, fut.conv, fut.dec, False)
        except asyncio.CancelledError:
            fut.cancel()
        except BaseException as exc:  # noqa: BLE001 - delivered to the caller
            if not fut.done():
                asyncio.Future.set_exception(fut, exc)
        else:
            if not fut.done():
                asyncio.Future.set_result(fut, reply)

    async def _route(self, conv: Conv, data: bytes, decode: Optional[bool]) -> Any:
        c = self._cl
        if c.mode != 1:
            if c.mode == 0:
                await self._discover()
            if c.mode == 2:
                return await Ruvio._x(self, conv, data, decode)
        dec = self._decode if decode is None else decode
        name, key = _frame_head(data)
        if not name.isupper():
            name = name.upper()
        if name not in _SPEC and name != b"FILLGET":
            if key is None:
                node = c.seed
            else:
                i = c.slots[_key_slot(key)]
                node = c.nodes[i] if i >= 0 else c.seed
            proto = node.proto
            if proto is None or proto.closed:
                proto = await self._open_node(node)
            fut = proto.loop.create_future()
            proto.pending.append((fut, conv, dec))
            proto.send(data)
            try:
                return await fut
            except MovedError as err:
                return await self._redirect(err, data, conv, dec, False)
        return await self._special(name, data, conv, dec)

    async def _redirect(self, err: MovedError, data: bytes, conv: Conv, dec: bool, blocking: bool) -> Any:
        """``-MOVED``: re-sends the command to the node the reply names, at most
        ``MAX_REDIRECTS`` times; the last ``MovedError`` is raised when they run out."""
        for hop in range(cl.MAX_REDIRECTS):
            mv = cl.parse_moved(err)
            if mv is None:
                raise err
            node = await self._moved_to(mv, hop)
            try:
                if blocking:
                    return await self._send_blocking(node, data, conv, dec)
                return await self._send(node, data, conv, dec)
            except MovedError as e:
                err = e
        raise err

    async def _moved_to(self, mv: Tuple[int, str, int], hop: int) -> _Node:
        """The node a ``MOVED`` names. The command's first ``MOVED`` to a node in the table points
        that one slot at it; an unknown node, or a second ``MOVED`` for the same command, reloads
        the whole table."""
        slot, host, port = mv
        c = self._cl
        node = c.by_addr.get((host, port))
        if node is None or node.idx < 0 or hop:
            await self._refresh()
            node = c.by_addr.get((host, port))
            if node is None:
                node = self._new_node(host, port)
                c.by_addr[(host, port)] = node
        else:
            c.slots[slot] = node.idx
        return node

    async def _single(self, slot: Optional[int], data: bytes, conv: Conv, dec: bool, blocking: bool) -> Any:
        node = self._owner(slot)
        try:
            if blocking:
                return await self._send_blocking(node, data, conv, dec)
            return await self._send(node, data, conv, dec)
        except MovedError as err:
            return await self._redirect(err, data, conv, dec, blocking)

    async def _special(self, name: bytes, data: bytes, conv: Conv, dec: bool) -> Any:
        args = cl.split_frame(data)
        p = cl.plan(name, args)
        kind = p[0]
        c = self._cl
        if kind == cl.PLAN_SLOT:
            if name == b"UNWATCH" and c.watch is not None:
                node, c.watch = c.watch, None
                return await self._send(node, data, conv, dec)
            reply = await self._single(p[1], data, conv, dec, _is_blocking(name, args))
            if name == b"WATCH":
                c.watch = self._owner(p[1])
            return reply
        if kind == cl.PLAN_SPLIT:
            return self._conv(conv, await self._split(name, args, p[1], dec))
        if kind == cl.PLAN_SCAN:
            return self._conv(conv, await self._scan(args, dec))
        return self._conv(conv, await self._fan(p[1], data, dec))

    @staticmethod
    def _conv(conv: Conv, reply: Any) -> Any:
        if isinstance(reply, ResponseError):
            raise reply
        return reply if conv is None else conv(reply)

    async def _gather(self, coros: List[Awaitable[Any]]) -> List[Any]:
        results = await asyncio.gather(*coros, return_exceptions=True)
        for r in results:
            if isinstance(r, BaseException) and not isinstance(r, ResponseError):
                raise r
        return list(results)

    async def _fan(self, mode: int, data: bytes, dec: bool) -> Any:
        results = await self._gather([self._send(n, data, None, dec) for n in self._cl.nodes])
        for r in results:
            if isinstance(r, ResponseError):
                raise r
        if mode == cl.FAN_SUM:
            return sum(results)
        if mode == cl.FAN_CAT:
            out: List[Any] = []
            for r in results:
                out.extend(r)
            return out
        return results[0]

    async def _split(self, name: bytes, args: List[bytes], keys: List[bytes], dec: bool) -> Any:
        groups = cl.split_groups(name, args, keys, self._owner_by_slot)
        order = list(groups.items())
        results = await self._gather([self._node_batch(node, [f for _, f in subs], dec) for node, subs in order])
        parts: List[Tuple[List[int], Any]] = []
        for (node, subs), r in zip(order, results):
            if isinstance(r, BaseException):
                raise r
            for (pos, frame), reply in zip(subs, r):
                if reply.__class__ is MovedError:
                    try:
                        reply = await self._redirect(reply, frame, None, dec, False)
                    except ResponseError as e:
                        reply = e
                parts.append((pos, reply))
        return cl.merge_split(name, len(keys), parts)

    def _owner_by_slot(self, slot: int) -> _Node:
        return self._owner(slot)

    async def _scan(self, args: List[bytes], dec: bool) -> Any:
        c = self._cl
        try:
            node_i, frame = cl.scan_prepare(args, len(c.nodes))
        except ResponseError as e:
            return e
        reply = await self._send(c.nodes[node_i], frame, None, dec)
        return cl.scan_finish(reply, node_i, len(c.nodes))

    # ---------------------------------------------------------------- pipelines
    def _batch(self, payload: List[bytes]) -> "Awaitable[List[Any]]":
        if self._cl.mode == 2:
            return Ruvio._batch(self, payload)
        return self._cbatch(payload)

    async def _cbatch(self, payload: List[bytes]) -> List[Any]:
        c = self._cl
        if c.mode != 1:
            if c.mode == 0:
                await self._discover()
            if c.mode == 2:
                return await Ruvio._batch(self, payload)
        dec = self._decode
        if payload[0] is _MULTI and payload[-1] is _EXEC:
            return await self._batch_tx(payload, dec)
        groups, subs = cl.pipeline_groups(payload, c.slots)  # raises before anything is sent
        order = [((c.nodes[ni] if ni >= 0 else c.seed), pos, frames) for ni, (pos, frames) in groups.items()]
        results = await self._gather([self._node_batch(node, frames, dec) for node, _, frames in order])
        n = len(payload)
        replies: List[Any] = [None] * (n if subs is None else n + len(subs))
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
                frame = payload[i] if i < n else subs[i - n][4]  # type: ignore[index]
                try:
                    replies[i] = await self._redirect(v, frame, None, dec, False)
                except ResponseError as e:
                    replies[i] = e
        return replies if subs is None else cl.merge_pipeline(replies, n, subs)

    async def _batch_tx(self, payload: List[bytes], dec: bool) -> List[Any]:
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
        replies: List[Any] = []
        for hop in range(cl.MAX_REDIRECTS + 1):
            replies = await self._node_batch(node, payload, dec)
            moved = next((r for r in replies if r.__class__ is MovedError), None)
            mv = None if moved is None or hop == cl.MAX_REDIRECTS else cl.parse_moved(moved)
            if mv is None:
                break
            # nothing ran (MOVED commands are refused at queue time): the whole transaction follows
            node = await self._moved_to(mv, hop)
        return replies

    # ---------------------------------------------------------------- connection-wide state
    def _share_opts(self) -> None:
        o = self._st.opts
        c = self._cl
        for n in {id(x): x for x in [c.seed, *c.nodes, *c.by_addr.values()]}.values():
            no = n.opts
            no.password, no.username, no.db, no.client_name = o.password, o.username, o.db, o.client_name

    async def select(self, db: int) -> Any:  # type: ignore[override]
        ok = await super().select(db)
        self._share_opts()
        return ok

    async def client_setname(self, name: str) -> Any:  # type: ignore[override]
        ok = await super().client_setname(name)
        self._share_opts()
        return ok

    async def auth(self, password: str, username: Optional[str] = None) -> Any:  # type: ignore[override]
        ok = await super().auth(password, username)
        self._share_opts()
        return ok

    @property
    def raw(self) -> "ClusterRuvio":
        view = ClusterRuvio.__new__(ClusterRuvio)
        view._st = self._st
        view._cl = self._cl
        view._decode = False
        return view

    async def connect(self) -> "ClusterRuvio":
        if self._cl.mode == 0:
            await self._discover()
        elif self._cl.mode == 2:
            await Ruvio.connect(self)
        return self

    async def aclose(self) -> None:
        c = self._cl
        for n in {id(x): x for x in [c.seed, *c.nodes, *c.by_addr.values()]}.values():
            proto, n.proto = n.proto, None
            if proto is not None and proto.transport is not None:
                proto.transport.close()
        c.watch = None
        await Ruvio.aclose(self)

    close = aclose

    def dedicated(self) -> "ClusterRuvio":
        """A new cluster client (own connections) with the same options - for WATCH and blocking commands."""
        opts = {k: v for k, v in self._st.opts.as_dict().items() if k != "socket_timeout"}
        return ClusterRuvio(decode_responses=self._decode, cluster=True, **opts)

    async def pubsub(self, ignore_subscribe_messages: bool = False) -> "ClusterPubSub":  # type: ignore[override]
        """Pub/Sub on one node: classic channels/patterns on the owner of slot 0 (where ``PUBLISH``
        goes); ``ssubscribe`` before anything else connects to the owner of the channel's slot."""
        if self._cl.mode == 0:
            await self._discover()
        ps = ClusterPubSub(self, ignore_subscribe_messages)
        await ps.connect()
        return ps


class ClusterPubSub(PubSub):
    def __init__(self, client: ClusterRuvio, ignore_subscribe_messages: bool = False) -> None:
        c = client._cl
        node = client._owner(0) if c.mode == 1 else c.seed
        opts = _Options(**client._st.opts.as_dict())
        opts.host, opts.port = node.host, node.port
        super().__init__(opts, client._decode, ignore_subscribe_messages)
        self._client = client

    async def ssubscribe(self, *channels: Any) -> None:
        c = self._client._cl
        if c.mode == 1 and channels:
            slots = {cl.key_slot(ch) for ch in channels}
            if len(slots) > 1:
                raise CrossSlotError("CROSSSLOT sharded channels of one SSUBSCRIBE must share a slot")
            node = self._client._owner(slots.pop())
            o = self._opts
            if (o.host, o.port) != (node.host, node.port):
                if self.channels or self.patterns or self.shard_channels:
                    raise RuvioError(
                        f"this Pub/Sub session is connected to {o.host}:{o.port} but the channel lives on "
                        f"{node.host}:{node.port}; open a separate pubsub() for it"
                    )
                await self.aclose()
                o.host, o.port = node.host, node.port
        await super().ssubscribe(*channels)
