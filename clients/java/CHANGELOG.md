# Changelog

Notable changes to the Java client `io.github.salihcantekin:ruvio-client`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- `replicaOf(host, port)`, `replicaOfNoOne(force)` and `role()` for `REPLICAOF` and `ROLE`, async and blocking.
- `RuvioOptions.Builder.seeds(...)`: `host:port` seed addresses tried in order instead of host and port. A dropped connection reconnects starting with the next seed; a cluster reloads its slot map from the other known nodes and then the seeds when no open socket answers.
- TTL on create: `Duration` overloads `incr`, `decr`, `incrby`, `decrby`, `incrbyMax`, `incrbyfloat`, `hincrby`, `zincrby`, `setbit` and `geoadd(key, nx, xx, ch, ttl, members)`, `jsonSet`/`jsonSetNx`/`jsonSetXx(key, path, json, ttl)`, and `ttl(Duration)` on `ZAddArgs`, `XAddArgs` and `BfInsertArgs`. The deadline is set only when the call creates the key; whole seconds are sent as `EX`, anything finer as `PX`.
- `expire`, `pexpire`, `expireat` and `pexpireat` overloads take an `ExpireCondition` (`NX`, `XX`, `GT`, `LT`); `ExpireCondition` now also has `fragment()`, like the other keyword enums.
- `zpopbyscore` and `zmovedue` (async and blocking) send `ZPOPBYSCORE` and `ZMOVEDUE`: take the due members of a sorted set (score up to a bound), or move them to the tail of a list, in one call.
- New `transfer(source, destination, amount[, max])` and `htransfer(key, from, to, amount[, max])` for the server's `TRANSFER` and `HTRANSFER`: they return the two balances after the move, or `null` when nothing moved.

- `limitSliding`, `limitTokens`, `limitSlidingWithRetry` and `limitTokensWithRetry` for the `SLIDING` and `TOKENS` modes of `LIMIT`, and the `LimitOutcome` record (needs a server that has them).

## 0.1.2 - 2026-10-17

- Typed keyword constants: new enums `InsertPosition` (`linsert`), `BitUnit` (`bitcount`), `GeoOrder` (`GeoSearchArgs.order`) and `KeyType` (`ScanArgs.type`), and `BitFieldArgs.Type.signed(n)` / `unsigned(n)` for BITFIELD types. The existing boolean and string overloads still work. Every enum, including `GeoUnit`, `ListDirection`, `BitOp`, `Overflow` and `Aggregate`, writes a pre-encoded constant, so the keyword costs no per-call encoding.
- More typed constants: `StreamTrim` (strategy `MAXLEN`/`MINID`, mode `=`/`~`) for `XAddArgs.trim` and the new `xtrim(key, StreamTrim)`, `InfoSection` for `info(InfoSection)`, `ClientType` for `clientList(ClientType)`, plus the id and bound helpers `StreamIds` and `RangeBounds`. The Java client has no `CHANGES SUBSCRIBE` call, so there is no change-type set. The existing overloads still work.

## 0.1.1 - 2026-10-17

- Cluster commands no longer copy their encoded frame for a possible `MOVED` retry, and a cluster pipeline finds its node groups by index and reuses them between flushes. Pipeline allocation per request fell by half to three quarters; see `docs/evidence/2026-10-16-client-cluster-routing.md`.
- Behaviour change: a cluster command now follows up to 3 `MOVED` redirects instead of 1 (the 4th is returned as the error). A `MOVED` points only that slot at the named node; the whole slot table is re-read with `CLUSTER SLOTS` only when that node is unknown or from the second `MOVED` for the same command on, and the retry always goes to the node the `MOVED` named (before, any second `MOVED` from any command triggered a reload).
- The cluster client now takes each command's key positions from the server's command table (`CommandKeyRules`, generated from `clients/testdata/commands.json`) instead of a hand-kept list, and a test checks them against the server's key vectors. Every vector already matched, so no command was routed wrong before; words the server does not know (such as `BRPOPLPUSH`, `LCS`, `ZRANGESTORE`, `ZMPOP`) now take the unknown-command default, the first argument.

## 0.1.0

- First release.
