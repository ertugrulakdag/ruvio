# Changelog

Notable changes to the Python client `ruvio-client` on PyPI. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- Added keyword-only `ttl=` (seconds as `int`/`float`, or `timedelta`) to `incr`, `decr`, `incrby`, `decrby`, `incrbyfloat`, `hincrby`, `zincrby`, `setbit`, `zadd`, `geoadd`, `xadd`, `json_set`, `json_set_obj` and `bf_insert`. The deadline is set only when the call creates the key; an existing key keeps its deadline. Whole seconds send `EX`, a sub-second part sends `PX`; zero or negative raises `ValueError`.
- Added `ExpireCondition` (`NX`, `XX`, `GT`, `LT`) and the optional `condition=` argument on `expire`, `pexpire`, `expireat` and `pexpireat`. A key without a deadline counts as infinite; a failed condition returns `False`.

## 0.1.2 - 2026-10-17

- Added `StreamTrim` (`MAXLEN` or `MINID`, `=` or `~`, threshold) for `xadd(trim=...)` and `xtrim(trim=...)`, the `InfoSection`, `ClientType` and `ChangeType` enums (with `change_types()`), `StreamId`, `ScoreBound` and `LexBound` constants, and `score_exclusive()`, `lex_inclusive()` and `lex_exclusive()` bound builders. Existing string arguments keep working.
- Added typed keyword constants (`GeoUnit`, `GeoOrder`, `InsertPosition`, `ListSide`, `BitOperation`, `BitUnit`, `Aggregate`, `KeyType`, `BitFieldOp`, `Overflow`, `signed`, `unsigned`). They are `str` enums, so existing string arguments keep working; the encoder sends each member's pre-encoded bytes.

## 0.1.1 - 2026-10-17

- Cluster routing is cheaper: the command name and first key are read with one match per frame, a pipeline is routed in one pass into reused per-node buffers, every node is written before any is read, and a pipeline for one node goes out unchanged. Cluster CPU per pipelined request fell 8-17%; see `docs/evidence/2026-10-16-client-cluster-routing.md`.
- Fixed: with the optional `hiredis` parser installed, commands that answer `OK` (`set`, `select`, `auth`, `client_setname` and others) returned `False` instead of `True` when replies were not decoded (`decode_responses=False` or the `binary` view), because hiredis returns the reply as `b"OK"`.
- **Behavior change:** the cluster client now follows up to 3 `-MOVED` redirects per command (was 1) before raising `MovedError`. A command's first `MOVED` to a known node updates only that slot; the whole slot map is re-read only when the named node is unknown or the same command gets a second `MOVED` (before, any second `MOVED` since the last reload re-read it). Transactions follow the same limit.
- **Behavior change:** a cluster pipeline (not a transaction) containing `MGET`, `MSET`, `DEL`, `EXISTS`, `UNLINK` or `TOUCH` with keys in several slots is no longer refused with `CrossSlotError`: the command is split per slot like the plain command, and its place in the reply list holds the merged reply. A transaction must still use one slot.
- The cluster client now takes each command's key positions from the server's command table instead of its own hand-written lists, and is tested against the server's key vectors. No server command was routed by the wrong keys before; commands the server does not have (`BRPOPLPUSH`, `BZMPOP`, `EVAL_RO`, `EVALSHA_RO`, `FCALL_RO`, `LCS`, `ZINTERCARD`, `ZMPOP`, `ZRANGESTORE`, `LASTSAVE`, `QUIT`, `RESET`, `TIME`) now go by argument 1 like any other unknown command.

## 0.1.0

- First release.
