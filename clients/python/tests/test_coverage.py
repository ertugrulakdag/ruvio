# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Fails when a command in docs/compatibility-matrix.json has no typed method.

The table maps each matrix command to the method that sends it. Methods marked with
``PubSub.`` live on the Pub/Sub session classes. The test also checks that every mapped
method exists on both the sync and the asyncio client, and that the live tests call it.
"""

from __future__ import annotations

import json
import os
import re

import ruvio
import ruvio.asyncio

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
MATRIX = os.path.join(ROOT, "docs", "compatibility-matrix.json")
TESTS = os.path.dirname(__file__)

COMMANDS = {
    # connection / acl
    "RUVIO": "ruvio", "PING": "ping", "HELLO": "hello", "AUTH": "auth", "SELECT": "select", "ECHO": "echo",
    "COMMAND": "command", "CLIENT GETNAME": "client_getname", "CLIENT SETNAME": "client_setname",
    "CLIENT TRACKING": "client_tracking", "CLIENT ID": "client_id", "CLIENT LIST": "client_list",
    "CLIENT INFO": "client_info", "CLIENT KILL": "client_kill", "SLOWLOG": "slowlog_get", "LATENCY": "latency_latest",
    "ACL WHOAMI": "acl_whoami", "ACL USERS": "acl_users", "ACL LIST": "acl_list", "ACL GETUSER": "acl_getuser",
    "ACL CAT": "acl_cat", "ACL SETUSER": "acl_setuser", "ACL LOAD": "acl_load", "ACL SAVE": "acl_save",
    # strings
    "GET": "get", "SET": "set", "SETV": "setv", "TAKE": "take", "DELEX": "delex", "GETEX": "getex",
    "MGET": "mget", "MSET": "mset", "MSETNX": "msetnx", "GETSET": "getset", "APPEND": "append",
    "STRLEN": "strlen", "INCR": "incr", "DECR": "decr", "INCRBY": "incrby", "DECRBY": "decrby",
    "INCRBYFLOAT": "incrbyfloat", "SETEX": "setex", "PSETEX": "psetex", "SETNX": "setnx", "GETDEL": "getdel",
    "GETRANGE": "getrange", "SUBSTR": "substr", "SETRANGE": "setrange", "FILLGET": "fillget", "FILLSET": "fillset",
    # keys / Ruvio primitives
    "LIMIT": "limit", "IDEM": "idem_begin", "ONCE": "once", "LEASE": "lease", "SEMAPHORE": "semaphore",
    "RELEASE": "release", "CHANGES": "changes", "DEL": "delete", "UNLINK": "unlink", "EXISTS": "exists",
    "TYPE": "type", "RENAME": "rename", "RENAMENX": "renamenx", "SCAN": "scan", "DBSIZE": "dbsize",
    "FLUSHDB": "flushdb", "FLUSHALL": "flushall", "SWAPDB": "swapdb", "MOVE": "move", "EXPIRE": "expire",
    "PEXPIRE": "pexpire", "EXPIREAT": "expireat", "PEXPIREAT": "pexpireat", "TTL": "ttl", "PTTL": "pttl",
    "PERSIST": "persist", "EXPIRETIME": "expiretime", "PEXPIRETIME": "pexpiretime", "KEYS": "keys",
    "RANDOMKEY": "randomkey", "TOUCH": "touch", "COPY": "copy", "OBJECT": "object_encoding",
    "MEMORY": "memory_usage", "DUMP": "dump", "RESTORE": "restore",
    # lists
    "LPUSH": "lpush", "RPUSH": "rpush", "LPUSHX": "lpushx", "RPUSHX": "rpushx", "LPUSHCAP": "lpushcap",
    "RPUSHCAP": "rpushcap", "LPOP": "lpop", "RPOP": "rpop", "LLEN": "llen", "LINDEX": "lindex",
    "LRANGE": "lrange", "LTRIM": "ltrim", "BLPOP": "blpop", "BRPOP": "brpop", "LINSERT": "linsert",
    "LSET": "lset", "LREM": "lrem", "LPOS": "lpos", "LMOVE": "lmove", "RPOPLPUSH": "rpoplpush",
    "BLMOVE": "blmove", "LMPOP": "lmpop", "BLMPOP": "blmpop",
    # sets
    "SADD": "sadd", "SREM": "srem", "SISMEMBER": "sismember", "SMISMEMBER": "smismember", "SCARD": "scard",
    "SMEMBERS": "smembers", "SINTER": "sinter", "SUNION": "sunion", "SDIFF": "sdiff",
    "SINTERSTORE": "sinterstore", "SUNIONSTORE": "sunionstore", "SDIFFSTORE": "sdiffstore",
    "SINTERCARD": "sintercard", "SMOVE": "smove", "SPOP": "spop", "SRANDMEMBER": "srandmember", "SSCAN": "sscan",
    # hashes
    "HSET": "hset", "HGET": "hget", "HDEL": "hdel", "HLEN": "hlen", "HGETALL": "hgetall", "HMGET": "hmget",
    "HEXISTS": "hexists", "HKEYS": "hkeys", "HVALS": "hvals", "HINCRBY": "hincrby",
    "HINCRBYFLOAT": "hincrbyfloat", "HSETNX": "hsetnx", "HSTRLEN": "hstrlen", "HSCAN": "hscan",
    "HRANDFIELD": "hrandfield", "HEXPIRE": "hexpire", "HPEXPIRE": "hpexpire", "HEXPIREAT": "hexpireat",
    "HPEXPIREAT": "hpexpireat", "HTTL": "httl", "HPTTL": "hpttl", "HEXPIRETIME": "hexpiretime",
    "HPEXPIRETIME": "hpexpiretime", "HPERSIST": "hpersist",
    # sorted sets
    "ZADD": "zadd", "ZRANGE": "zrange", "ZREVRANGE": "zrevrange", "ZRANGEBYSCORE": "zrangebyscore",
    "ZRANGEBYLEX": "zrangebylex", "ZREVRANGEBYLEX": "zrevrangebylex", "ZREM": "zrem", "ZINCRBY": "zincrby",
    "ZCARD": "zcard", "ZSCORE": "zscore", "ZMSCORE": "zmscore", "ZRANK": "zrank", "ZREVRANK": "zrevrank",
    "ZCOUNT": "zcount", "ZLEXCOUNT": "zlexcount", "ZPOPMIN": "zpopmin", "ZPOPMAX": "zpopmax",
    "BZPOPMIN": "bzpopmin", "BZPOPMAX": "bzpopmax", "ZREMRANGEBYRANK": "zremrangebyrank",
    "ZREMRANGEBYSCORE": "zremrangebyscore", "ZREMRANGEBYLEX": "zremrangebylex", "ZUNIONSTORE": "zunionstore",
    "ZINTERSTORE": "zinterstore", "ZDIFFSTORE": "zdiffstore", "ZUNION": "zunion", "ZINTER": "zinter",
    "ZDIFF": "zdiff", "ZRANDMEMBER": "zrandmember", "ZSCAN": "zscan",
    # geo
    "GEOADD": "geoadd", "GEODIST": "geodist", "GEOHASH": "geohash", "GEOPOS": "geopos",
    "GEOSEARCH": "geosearch", "GEOSEARCHSTORE": "geosearchstore", "GEORADIUS": "georadius",
    "GEORADIUSBYMEMBER": "georadiusbymember",
    # bloom
    "BF.RESERVE": "bf_reserve", "BF.ADD": "bf_add", "BF.MADD": "bf_madd", "BF.INSERT": "bf_insert",
    "BF.INFO": "bf_info", "BF.CARD": "bf_card", "BF.EXISTS": "bf_exists", "BF.MEXISTS": "bf_mexists",
    # json
    "JSON.GET": "json_get", "JSON.SET": "json_set", "JSON.MGET": "json_mget", "JSON.DEL": "json_del",
    "JSON.FORGET": "json_forget", "JSON.MERGE": "json_merge", "JMERGE": "jmerge", "JSON.TYPE": "json_type",
    "JSON.STRLEN": "json_strlen", "JSON.ARRLEN": "json_arrlen", "JSON.OBJKEYS": "json_objkeys",
    "JSON.OBJLEN": "json_objlen", "JSON.NUMINCRBY": "json_numincrby", "JSON.NUMMULTBY": "json_nummultby",
    "JSON.TOGGLE": "json_toggle", "JSON.ARRAPPEND": "json_arrappend", "JSON.ARRINSERT": "json_arrinsert",
    "JSON.ARRPOP": "json_arrpop", "JSON.ARRTRIM": "json_arrtrim", "JSON.ARRINDEX": "json_arrindex",
    "JSON.CLEAR": "json_clear", "JSON.STRAPPEND": "json_strappend", "JSON.INDEX": "json_index_create",
    "JSON.FIND": "json_find",
    # streams
    "XADD": "xadd", "XLEN": "xlen", "XRANGE": "xrange", "XREVRANGE": "xrevrange", "XREAD": "xread",
    "XDEL": "xdel", "XSETID": "xsetid", "XTRIM": "xtrim", "XGROUP CREATE": "xgroup_create",
    "XGROUP CREATECONSUMER": "xgroup_createconsumer", "XREADGROUP": "xreadgroup",
    "XGROUP DESTROY": "xgroup_destroy", "XGROUP SETID": "xgroup_setid",
    "XGROUP DELCONSUMER": "xgroup_delconsumer", "XACK": "xack", "XPENDING": "xpending",
    "XAUTOCLAIM": "xautoclaim", "XCLAIM": "xclaim", "XINFO STREAM": "xinfo_stream",
    "XINFO GROUPS": "xinfo_groups", "XINFO CONSUMERS": "xinfo_consumers",
    # server
    "INFO": "info", "WAIT": "wait", "CONFIG GET": "config_get", "SAVE": "save", "BGSAVE": "bgsave",
    # transactions
    "MULTI": "multi", "EXEC": "exec", "DISCARD": "discard", "WATCH": "watch", "UNWATCH": "unwatch",
    # scripts
    "EVAL": "eval", "EVALSHA": "evalsha", "SCRIPT LOAD": "script_load", "SCRIPT EXISTS": "script_exists",
    "SCRIPT FLUSH": "script_flush", "SCRIPT KILL": "script_kill", "FCALL": "fcall",
    "FUNCTION LOAD": "function_load", "FUNCTION LIST": "function_list", "FUNCTION DELETE": "function_delete",
    # cluster
    "CLUSTER SLOTS": "cluster_slots", "CLUSTER NODES": "cluster_nodes", "CLUSTER SHARDS": "cluster_shards",
    "CLUSTER INFO": "cluster_info", "CLUSTER MYID": "cluster_myid", "CLUSTER KEYSLOT": "cluster_keyslot",
    "READONLY": "readonly", "READWRITE": "readwrite",
    # pub/sub
    "PUBLISH": "publish", "SPUBLISH": "spublish", "SUBSCRIBE": "PubSub.subscribe",
    "UNSUBSCRIBE": "PubSub.unsubscribe", "PSUBSCRIBE": "PubSub.psubscribe",
    "PUNSUBSCRIBE": "PubSub.punsubscribe", "SSUBSCRIBE": "PubSub.ssubscribe",
    "SUNSUBSCRIBE": "PubSub.sunsubscribe",
    # bitmaps / hyperloglog
    "SETBIT": "setbit", "GETBIT": "getbit", "BITCOUNT": "bitcount", "BITPOS": "bitpos", "BITOP": "bitop",
    "BITFIELD": "bitfield", "BITFIELD_RO": "bitfield_ro", "PFADD": "pfadd", "PFCOUNT": "pfcount",
    "PFMERGE": "pfmerge",
}

# Sub-forms and options with their own methods (also checked for existence and live use).
EXTRA = [
    "command_count", "command_info", "command_docs", "slowlog_len", "slowlog_reset", "idem_complete",
    "changes_start", "object_idletime", "object_freq", "json_index_drop", "json_index_list", "xpending_range",
    "rate_limit", "seen_before", "push_recent", "json_get_obj", "json_set_obj", "json_mget_obj",
    "execute_command", "lock", "lease_scope", "semaphore_scope", "get_or_create", "idempotent",
    "scan_iter", "hscan_iter", "sscan_iter", "zscan_iter", "pipeline", "transaction", "pubsub", "dedicated",
]


def _matrix():
    with open(MATRIX, encoding="utf-8") as f:
        return [c["name"] for c in json.load(f)["commands"]]


def _live_sources():
    text = ""
    for name in os.listdir(TESTS):
        if name.startswith("test_") and name.endswith(".py") and name not in ("test_coverage.py", "test_resp.py"):
            with open(os.path.join(TESTS, name), encoding="utf-8") as f:
                text += f.read()
    return text


def _resolve(cls, pubsub_cls, method):
    if method.startswith("PubSub."):
        return getattr(pubsub_cls, method.split(".", 1)[1], None)
    return getattr(cls, method, None)


def test_every_matrix_command_has_a_typed_method():
    names = _matrix()
    missing = [n for n in names if n not in COMMANDS]
    assert not missing, f"no typed method mapped for: {missing}"
    for cls, ps in ((ruvio.Ruvio, ruvio.PubSub), (ruvio.asyncio.Ruvio, ruvio.asyncio.PubSub)):
        absent = [f"{n} -> {m}" for n, m in COMMANDS.items() if not callable(_resolve(cls, ps, m))]
        assert not absent, f"{cls.__module__}: methods missing: {absent}"
    stale = sorted(set(COMMANDS) - set(names))
    assert not stale, f"mapped commands not in the matrix: {stale}"
    print(f"coverage: {sum(1 for n in names if n in COMMANDS)}/{len(names)}")


def test_extra_methods_exist():
    for cls in (ruvio.Ruvio, ruvio.asyncio.Ruvio):
        absent = [m for m in EXTRA if not callable(getattr(cls, m, None))]
        assert not absent, f"{cls.__name__}: {absent}"


def test_every_typed_method_is_exercised_by_live_tests():
    src = _live_sources()
    unused = []
    for m in list(COMMANDS.values()) + EXTRA:
        name = m.split(".", 1)[-1]
        if not re.search(r"\b" + re.escape(name) + r"\(", src):
            unused.append(m)
    assert not unused, f"typed methods never called by a live test: {unused}"
