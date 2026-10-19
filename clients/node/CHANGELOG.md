# Changelog

Notable changes to the Node.js client `ruvio-client` on npm. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- TTL on create: an optional `ttl` (seconds; whole numbers send `EX`, fractions send `PX`) on `incr`, `decr`, `incrby`, `decrby`, `incrbyMax`, `incrbyfloat`, `hincrby`, `zincrby`, `setbit`, `jsonSet`, `jsonSetAs`, and in `ZAddOptions`, `GeoAddOptions` (new type), `XAddOptions` and `BfInsertOptions`. The deadline is set only when the call creates the key. A `ttl` that is not positive and finite rejects with `RangeError` before anything is sent. New `TtlOptions` type.
- `expire`, `pexpire`, `expireat` and `pexpireat` take an optional trailing `ExpireCondition` (`NX`, `XX`, `GT`, `LT`).
- `zpopbyscore` and `zmovedue` send `ZPOPBYSCORE` and `ZMOVEDUE`: take the due members of a sorted set (score up to a bound), or move them to the tail of a list, in one call.
- New `transfer(source, destination, amount, max?)` and `htransfer(key, from, to, amount, max?)` for the server's `TRANSFER` and `HTRANSFER`: they return `[a, b]` after the move, or `null` when nothing moved.

- `limitSliding`, `limitTokens`, `limitSlidingWithRetry` and `limitTokensWithRetry` for the `SLIDING` and `TOKENS` modes of `LIMIT` (needs a server that has them).

## 0.2.0 - 2026-10-17

- Named keyword constants next to the string unions (`GeoUnit.Km`, `GeoOrder`, `ListSide`, `InsertPosition`, `BitOp`, `BitUnit`, `Aggregate`, `ScanType`, `ExpireCondition`), sent from pre-encoded buffers. `ScanOptions.type` is typed by `ScanType`; `BitfieldType` is now `` `i${number}` | `u${number}` ``; `BitfieldOp` builds `BITFIELD` sub-operations. String literals keep working.
- `StreamTrim` (`MAXLEN` / `MINID`, exact `=` or approximate `~`) is accepted by `xtrim` and as `XAddOptions.trim`; `InfoSection` types `info()`, `ClientType` types `clientList()`; both send pre-encoded buffers. New helpers `StreamId` (`*`, `$`, `>`, `-`, `+`), `ScoreBound` and `LexBound` return the strings the range methods accept. The client has no `CHANGES SUBSCRIBE` API, so no change-type set was added.

## 0.1.2 - 2026-10-17

- Cluster routing is cheaper: a command whose first argument is its key is routed without scanning the key rules, a cluster batch is routed in one pass into reused scratch and written unchanged when every command goes to one node, and only error replies are checked for `MOVED`. See `docs/evidence/2026-10-16-client-cluster-routing.md`.
- The cluster client now takes each command's key positions from the server's command table instead of its own hand-kept lists. This fixes routing of `GEORADIUS` and `GEORADIUSBYMEMBER` with `STORE` / `STOREDIST`, which used to ignore the destination key.
- **Behaviour change:** the cluster client now follows up to 3 `MOVED` redirects per command (was 1) before returning the `MOVED` error. A `MOVED` to a known node only repoints that slot; the whole slot table is re-read only when the named node is unknown or on the second `MOVED` for the same command (it used to be re-read once a second slot had moved, whichever command saw it).

## 0.1.1

- Same code as 0.1.0.

## 0.1.0

- First release.
