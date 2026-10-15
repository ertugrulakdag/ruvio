# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Cluster routing logic shared by the sync and asyncio clients (pure functions, no I/O).

Commands arrive at the router as complete RESP frames (``bytes``). Only the command name and
the first key are read for the common single-key case; every other command is split into
arguments and run through :func:`plan`.
"""

from __future__ import annotations

import binascii
from typing import Any, Dict, List, Optional, Tuple, Union

from ._resp import encode, encode_args
from .errors import CrossSlotError, ResponseError, RuvioError

SLOT_COUNT = 16384

_crc16 = binascii.crc_hqx  # CRC-CCITT with poly 0x1021 and init 0 == CRC16/XMODEM, in C


def key_slot(key: Union[str, bytes]) -> int:
    """Hash slot of ``key``: CRC16-XMODEM of the key, or of its first non-empty ``{hashtag}``, mod 16384."""
    if type(key) is not bytes:
        key = key.encode("utf-8", "surrogateescape") if isinstance(key, str) else bytes(key)
    o = key.find(b"{")
    if o >= 0:
        c = key.find(b"}", o + 1)
        if c > o + 1:
            key = key[o + 1 : c]
    return _crc16(key, 0) & 0x3FFF


# --------------------------------------------------------------------------- frames


def frame_head(data: bytes) -> Tuple[bytes, Optional[bytes]]:
    """``(name, first_argument)`` of an encoded command without parsing the rest."""
    i = data.find(b"\r\n")
    argc = int(data[1:i])
    j = data.find(b"\r\n", i + 2)
    n = int(data[i + 3 : j])
    s = j + 2
    name = data[s : s + n]
    if argc < 2:
        return name, None
    p = s + n + 2
    k = data.find(b"\r\n", p)
    m = int(data[p + 1 : k])
    return name, data[k + 2 : k + 2 + m]


def split_frame(data: bytes) -> List[bytes]:
    """All arguments of an encoded command (the command name is element 0)."""
    i = data.find(b"\r\n")
    pos = i + 2
    out: List[bytes] = []
    find = data.find
    for _ in range(int(data[1:i])):
        j = find(b"\r\n", pos)
        s = j + 2
        e = s + int(data[pos + 1 : j])
        out.append(data[s:e])
        pos = e + 2
    return out


# --------------------------------------------------------------------------- key positions

# kinds of key layout. A command that is not listed has exactly one key: argument 1.
_KEYLESS = 1  # no key: any node
_ALL = 2  # every argument after the name is a key
_TWO = 3  # arguments 1 and 2 are keys
_MSET = 4  # key value key value ...
_LASTISTIMEOUT = 5  # BLPOP k1 k2 ... timeout
_COUNT1 = 6  # name numkeys key...
_COUNT2 = 7  # name x numkeys key...
_STORECOUNT = 8  # name dest numkeys key...
_STREAMS = 9  # ... STREAMS k1 k2 id1 id2
_SUB2 = 10  # name subcommand key ...
_BITOP = 11  # BITOP op dest src...
_LASTISPATH = 12  # JSON.MGET k1 k2 ... path
_GEO = 13  # GEORADIUS key ... [STORE key]
# commands with their own routing
_FAN_SUM = 20  # DBSIZE: ask every node, add the integers
_FAN_FIRST = 21  # FLUSHDB, AUTH ...: run on every node, return one reply
_FAN_CAT = 22  # KEYS: ask every node, concatenate
_SCAN = 23
_SLOT0 = 24  # PUBLISH: classic pub/sub meets on the owner of slot 0
_TXN = 25  # MULTI / EXEC / DISCARD sent bare
_COND = 26  # CLIENT / SCRIPT / FUNCTION: only some sub-commands run everywhere

SPEC: Dict[bytes, int] = {}

SPEC.update({n.encode(): _KEYLESS for n in (
    "ACL BGSAVE CHANGES CLUSTER COMMAND CONFIG ECHO HELLO INFO JSON.FIND JSON.INDEX LASTSAVE LATENCY PING "
    "PSUBSCRIBE PUNSUBSCRIBE QUIT RANDOMKEY READONLY READWRITE RESET RUVIO SAVE SLOWLOG SUBSCRIBE SWAPDB TIME "
    "UNSUBSCRIBE WAIT"
).split()})
SPEC.update({n.encode(): _ALL for n in (
    "DEL EXISTS MGET PFCOUNT PFMERGE SDIFF SDIFFSTORE SINTER SINTERSTORE SSUBSCRIBE SUNION SUNIONSTORE SUNSUBSCRIBE "
    "TOUCH UNLINK WATCH"
).split()})
SPEC.update({n.encode(): _TWO for n in (
    "BLMOVE BRPOPLPUSH COPY LMOVE RENAME RENAMENX RPOPLPUSH SMOVE GEOSEARCHSTORE ZRANGESTORE LCS"
).split()})
SPEC.update({b"MSET": _MSET, b"MSETNX": _MSET})
SPEC.update({n.encode(): _LASTISTIMEOUT for n in "BLPOP BRPOP BZPOPMIN BZPOPMAX".split()})
SPEC.update({n.encode(): _COUNT1 for n in "ZUNION ZINTER ZDIFF LMPOP SINTERCARD ZINTERCARD ZMPOP".split()})
SPEC.update({n.encode(): _COUNT2 for n in "BLMPOP BZMPOP EVAL EVALSHA EVAL_RO EVALSHA_RO FCALL FCALL_RO".split()})
SPEC.update({n.encode(): _STORECOUNT for n in "ZUNIONSTORE ZINTERSTORE ZDIFFSTORE".split()})
SPEC.update({b"XREAD": _STREAMS, b"XREADGROUP": _STREAMS})
SPEC.update({n.encode(): _SUB2 for n in "OBJECT MEMORY XGROUP XINFO".split()})
SPEC[b"BITOP"] = _BITOP
SPEC[b"JSON.MGET"] = _LASTISPATH
SPEC.update({b"GEORADIUS": _GEO, b"GEORADIUSBYMEMBER": _GEO})
SPEC.update({b"DBSIZE": _FAN_SUM, b"FLUSHDB": _FAN_FIRST, b"FLUSHALL": _FAN_FIRST, b"AUTH": _FAN_FIRST,
             b"SELECT": _FAN_FIRST, b"KEYS": _FAN_CAT, b"SCAN": _SCAN, b"PUBLISH": _SLOT0})
SPEC.update({b"MULTI": _TXN, b"EXEC": _TXN, b"DISCARD": _TXN, b"UNWATCH": _KEYLESS})
SPEC.update({b"CLIENT": _COND, b"SCRIPT": _COND, b"FUNCTION": _COND})

# (command, sub-command) pairs of CLIENT / SCRIPT / FUNCTION that must run on every node
_FAN_SUB = frozenset(
    (
        (b"CLIENT", b"SETNAME"),
        (b"SCRIPT", b"LOAD"),
        (b"SCRIPT", b"FLUSH"),
        (b"FUNCTION", b"LOAD"),
        (b"FUNCTION", b"DELETE"),
        (b"FUNCTION", b"FLUSH"),
    )
)

#: multi-key commands whose keys may live on several nodes: split per node and re-assembled
SPLITTABLE = frozenset((b"MGET", b"MSET", b"DEL", b"UNLINK", b"EXISTS", b"TOUCH"))

#: commands that park the connection until data arrives
BLOCKING = frozenset((b"BLPOP", b"BRPOP", b"BRPOPLPUSH", b"BLMOVE", b"BLMPOP", b"BZPOPMIN", b"BZPOPMAX", b"BZMPOP"))

PLAN_SLOT = 0  # (PLAN_SLOT, slot or None)
PLAN_SPLIT = 1  # (PLAN_SPLIT, keys)
PLAN_FAN = 2  # (PLAN_FAN, SPEC kind)
PLAN_SCAN = 3

FAN_SUM = _FAN_SUM
FAN_FIRST = _FAN_FIRST
FAN_CAT = _FAN_CAT

_UNSUPPORTED_TXN = (
    "MULTI/EXEC/DISCARD cannot be sent one by one on a cluster client (the queued commands may belong to "
    "different nodes); use pipeline(transaction=True) or transaction(fn, *watch_keys)"
)


def _counted(args: List[bytes], at: int) -> List[bytes]:
    if at >= len(args):
        return []
    try:
        n = int(args[at])
    except ValueError:
        return []
    return args[at + 1 : at + 1 + n]


def command_keys(name: bytes, args: List[bytes]) -> List[bytes]:
    """The keys of a command in argument order (empty for keyless commands).

    ``name`` is the upper-case first word, ``args`` the whole argument vector including it."""
    kind = SPEC.get(name)
    n = len(args)
    if kind is None:
        return args[1:2]
    if kind == _SUB2:
        return args[2:3]
    if n < 2:
        return []
    if kind == _ALL:
        return args[1:]
    if kind == _TWO:
        return args[1:3]
    if kind == _MSET:
        return args[1::2]
    if kind == _LASTISTIMEOUT or kind == _LASTISPATH:
        return args[1 : n - 1]
    if kind == _COUNT1:
        return _counted(args, 1)
    if kind == _COUNT2:
        return _counted(args, 2)
    if kind == _STORECOUNT:
        return [args[1]] + _counted(args, 2)
    if kind == _STREAMS:
        for i in range(1, n):
            if args[i].upper() == b"STREAMS":
                return args[i + 1 : i + 1 + (n - i - 1) // 2]
        return []
    if kind == _BITOP:
        return args[2:]
    if kind == _GEO:
        keys = [args[1]]
        for i in range(2, n - 1):
            if args[i].upper() in (b"STORE", b"STOREDIST"):
                keys.append(args[i + 1])
        return keys
    if kind == _SLOT0 or kind == _SCAN:
        return []
    return []


def plan(name: bytes, args: List[bytes]) -> Tuple[Any, ...]:
    """Decides how a command is routed. Raises :class:`CrossSlotError` before anything is sent."""
    kind = SPEC.get(name)
    if kind is None:
        return (PLAN_SLOT, key_slot(args[1]) if len(args) > 1 else None)
    if kind == _KEYLESS:
        return (PLAN_SLOT, None)
    if kind == _SLOT0:
        return (PLAN_SLOT, 0)
    if kind == _TXN:
        raise RuvioError(_UNSUPPORTED_TXN)
    if kind == _SCAN:
        return (PLAN_SCAN,)
    if kind == _FAN_SUM or kind == _FAN_FIRST or kind == _FAN_CAT:
        return (PLAN_FAN, kind)
    if kind == _COND:
        if len(args) > 1 and (name, args[1].upper()) in _FAN_SUB:
            return (PLAN_FAN, _FAN_FIRST)
        return (PLAN_SLOT, None)
    keys = command_keys(name, args)
    if not keys:
        return (PLAN_SLOT, None)
    first = key_slot(keys[0])
    for k in keys[1:]:
        if key_slot(k) != first:
            if name in SPLITTABLE:
                return (PLAN_SPLIT, keys)
            raise CrossSlotError(
                f"CROSSSLOT {name.decode()} keys don't hash to the same slot (nothing was sent)"
            )
    return (PLAN_SLOT, first)


def pipeline_slot(frame: bytes) -> Optional[int]:
    """Slot of one pipelined command (None = keyless). Fan-out and cross-slot commands are refused."""
    name, key = frame_head(frame)
    name = name.upper()
    kind = SPEC.get(name)
    if kind is None:
        return None if key is None else key_slot(key)
    p = plan(name, split_frame(frame))
    if p[0] == PLAN_SLOT:
        return p[1]
    if p[0] == PLAN_SPLIT:
        raise CrossSlotError(
            f"CROSSSLOT {name.decode()} keys don't hash to the same slot; split it or call it outside the pipeline"
        )
    raise RuvioError(f"{name.decode()} fans out to every node and cannot be pipelined on a cluster client")


def pipeline_groups(
    frames: List[bytes], table: List[int]
) -> Dict[int, Tuple[List[int], List[bytes]]]:
    """Groups pipelined frames by node index (``-1`` = keyless: the seed), in one pass.

    ``table`` is the slot -> node index list. Raises before anything is sent if a command
    cannot be routed (cross-slot multi-key command, fan-out command)."""
    groups: Dict[int, Tuple[List[int], List[bytes]]] = {}
    spec = SPEC
    for i, f in enumerate(frames):
        name, key = frame_head(f)
        if not name.isupper():
            name = name.upper()
        if name in spec:
            slot = pipeline_slot(f)
        else:
            slot = None if key is None else key_slot(key)
        ni = -1 if slot is None else table[slot]
        g = groups.get(ni)
        if g is None:
            g = groups[ni] = ([], [])
        g[0].append(i)
        g[1].append(f)
    return groups


# --------------------------------------------------------------------------- split commands


def split_groups(
    name: bytes, args: List[bytes], keys: List[bytes], node_of: Any
) -> Dict[Any, List[Tuple[List[int], bytes]]]:
    """Splits MGET/DEL/MSET... into one sub-command per slot (the server wants one slot per command)
    and groups the sub-commands by owning node.

    ``node_of(slot)`` returns a hashable node handle. Returns ``{node: [(key positions, frame)]}``."""
    pairs = name == b"MSET"
    by_slot: Dict[int, List[int]] = {}
    for i, k in enumerate(keys):
        by_slot.setdefault(key_slot(k), []).append(i)
    out: Dict[Any, List[Tuple[List[int], bytes]]] = {}
    for slot, pos in by_slot.items():
        if pairs:
            sub: List[bytes] = []
            for i in pos:
                sub.append(args[1 + 2 * i])
                sub.append(args[2 + 2 * i])
        else:
            sub = [keys[i] for i in pos]
        out.setdefault(node_of(slot), []).append((pos, encode_args((name, *sub))))
    return out


def merge_split(name: bytes, nkeys: int, parts: List[Tuple[List[int], Any]]) -> Any:
    """Re-assembles the per-node replies (``parts`` = ``[(positions, reply)]``) in the caller's order."""
    for _, r in parts:
        if isinstance(r, ResponseError):
            return r
    if name == b"MGET":
        merged: List[Any] = [None] * nkeys
        for pos, r in parts:
            for i, v in zip(pos, r):
                merged[i] = v
        return merged
    if name == b"MSET":
        return "OK"
    return sum(r for _, r in parts)


# --------------------------------------------------------------------------- SCAN across nodes


def scan_prepare(args: List[bytes], nodes: int) -> Tuple[int, bytes]:
    """The client cursor is ``server_cursor * nodes + node``: 0 starts on the first node and comes
    back as 0 only after the last one. Returns ``(node index, frame for that node)``."""
    try:
        cursor = int(args[1])
    except (IndexError, ValueError):
        raise ResponseError("ERR invalid cursor") from None
    if cursor < 0:
        raise ResponseError("ERR invalid cursor")
    node = cursor % nodes
    return node, encode_args((args[0], str(cursor // nodes).encode(), *args[2:]))


def scan_finish(reply: Any, node: int, nodes: int) -> Any:
    if not isinstance(reply, (list, tuple)) or len(reply) != 2:
        return reply
    nxt = int(reply[0])
    new = nxt * nodes + node if nxt else (node + 1 if node + 1 < nodes else 0)
    text = str(new)
    return [text.encode() if type(reply[0]) is bytes else text, reply[1]]


# --------------------------------------------------------------------------- topology

CLUSTER_SLOTS = encode("CLUSTER SLOTS", ())


class Topology:
    """16384-entry slot -> node index table plus the node addresses (in slot order)."""

    __slots__ = ("slots", "nodes")

    def __init__(self, slots: List[int], nodes: List[Tuple[str, int]]) -> None:
        self.slots = slots
        self.nodes = nodes


def _text(v: Any) -> str:
    return v.decode() if type(v) is bytes else str(v)


def parse_slots(reply: Any, seed_host: str) -> Optional[Topology]:
    """Reads a ``CLUSTER SLOTS`` reply. None for an error or an empty map."""
    if not isinstance(reply, list) or not reply:
        return None
    slots = [-1] * SLOT_COUNT
    nodes: List[Tuple[str, int]] = []
    index: Dict[Tuple[str, int], int] = {}
    try:
        for rng in sorted(reply, key=lambda r: int(r[0])):
            first, last, primary = int(rng[0]), int(rng[1]), rng[2]
            host = _text(primary[0])
            if host in ("", "0.0.0.0", "::"):
                host = seed_host
            addr = (host, int(primary[1]))
            idx = index.get(addr)
            if idx is None:
                idx = index[addr] = len(nodes)
                nodes.append(addr)
            for s in range(max(0, first), min(SLOT_COUNT - 1, last) + 1):
                slots[s] = idx
    except (IndexError, TypeError, ValueError):
        return None
    return Topology(slots, nodes)


def parse_moved(err: BaseException) -> Optional[Tuple[int, str, int]]:
    """``(slot, host, port)`` of a ``MOVED slot host:port`` error; None for anything else."""
    msg = err.args[0] if err.args else ""
    if not isinstance(msg, str) or not msg.startswith("MOVED "):
        return None
    parts = msg.split()
    if len(parts) < 3:
        return None
    host, _, port = parts[2].rpartition(":")
    try:
        return int(parts[1]), host, int(port)
    except ValueError:
        return None
