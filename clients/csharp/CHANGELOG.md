# Changelog

Notable changes to the C# packages `Ruvio.Client` and `Ruvio.Client.AspNetCore`, which share one version. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

## 0.2.8 to 0.3.2

These notes were recorded together and shipped over these versions.

- Coverage: typed methods for every command in `docs/compatibility-matrix.json` (287/287), including `SET ... IFEQ`, `DELEX`, `LPUSHCAP`/`RPUSHCAP`, `BF.MADD/INSERT/INFO/CARD/MEXISTS`, `BF.RESERVE EXPANSION/NONSCALING`, `JSON.SET NX/XX`, `JSON.INDEX`/`JSON.FIND`, the remaining `JSON.*`, bitmaps, HyperLogLog, `SSCAN`/`ZSCAN`, `ZUNION`/`ZINTER`/`ZDIFF`, `BLMPOP`, `COMMAND`, `LATENCY`, `WAIT`, `AUTH` and `RUVIO`. New `IRuvioClient` members have default implementations that go through `ExecuteAsync`, so existing implementations keep compiling.
- Helpers on `IRuvioClient`: `TryAcquireLockAsync`/`AcquireLockAsync` (`RuvioLock`, `IAsyncDisposable`), `TryAcquireLeaseAsync`, `TryAcquireSemaphoreAsync` (`RuvioLease`), `RateLimitAsync`, `GetOrCreateAsync`, `ScanKeysAsync`/`ScanHashEntriesAsync`/`ScanSetMembersAsync`/`ScanSortedSetEntriesAsync` (`IAsyncEnumerable`), `PushRecentAsync`/`GetRecentAsync`, `SeenBeforeAsync`/`MaybeSeenAsync`.
- `ExecuteAsync(string[])` and every list-path command encode the array through a span instead of the `IReadOnlyList<string>` interface.
- `MatrixCoverageTests` and the live `LiveCommandTests`/`LiveHelperTests` (`RUVIO_TEST_ADDR`).
- Build: the source generator references Microsoft.CodeAnalysis.CSharp 4.8.0 (was 4.11.0), so it loads in the .NET 8.0.1xx SDK compiler; before, the generator was skipped there and the client did not compile (`RespVerbs` missing).
- Breaking: every command method on `RuvioClient` / `IRuvioClient` returns `ValueTask` or `ValueTask<T>` instead of `Task` / `Task<T>`. A pending command is a pooled object that is reused once it completes. Await each result **once** and do not keep it after awaiting. Do not call `.Result` / `GetAwaiter().GetResult()` on a result that has not completed. To use `Task.WhenAll` / `WhenAny` or a `Task`-based interface, call `.AsTask()` once. Usual `await client.GetAsync(key)` code only needs to be compiled again. On a local memory server with 16 callers, this cut allocations from about 1.1–1.4 KB to 56–336 B per command, and the 336 B left on `GET` is mostly the reply payload.
- Frequent replies are shared rather than allocated: null, `+OK`, `+PONG`, `+QUEUED`, the empty bulk string, and integers from -2 to 255. `RespValue` is immutable, so this is not visible to callers, but do not compare replies by reference.
- Typed commands no longer build a `string[]` or format integers into strings. Arguments are stored inline in a `RespCommand` struct, and numbers are written straight to the buffer as invariant UTF-8. Simple-string replies such as `+OK` are matched without a string. A `SET` (typed or raw) now allocates 0 B per command; a `GET` allocates only the reply payload. Passing a `null` string argument now throws `ArgumentNullException` immediately instead of returning a faulted task.
- Hash field TTLs: `ExpireHashFieldsAsync`, `ExpireHashFieldsAtAsync`, `GetHashFieldTimeToLiveAsync`, `GetHashFieldTimeToLiveMillisecondsAsync`, `GetHashFieldExpireTimeAsync`, `GetHashFieldExpireTimeMillisecondsAsync`, and `PersistHashFieldsAsync` send `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HEXPIRETIME`, `HPEXPIRETIME`, and `HPERSIST`.
- `AcquireSemaphoreAsync` sends `SEMAPHORE`; `ReleaseAsync` frees a semaphore token as well as a lease. `IncrementWithLimitAsync` sends `INCRBY key delta MAX limit` and returns null when the limit would be passed.
- Sorted sets: `PopSortedSetMinAsync`, `PopSortedSetMaxAsync`, `PopSortedSetMinBlockingAsync`, `PopSortedSetMaxBlockingAsync`, `RemoveSortedSetRangeByRankAsync`, `RemoveSortedSetRangeByScoreAsync`, and `RemoveSortedSetRangeByLexAsync` send `ZPOPMIN`, `ZPOPMAX`, `BZPOPMIN`, `BZPOPMAX`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, and `ZREMRANGEBYLEX`.
- Cluster mode: a single-key command (`GET`, `SET`, `INCR`, …) is routed from its inline arguments without building a key list, and each topology keeps one socket per node instead of looking it up by an `"host:port"` string on every call. Pipelines that hit one shard are sent as they are; multi-shard pipelines no longer use LINQ.
- Array, integer-array, scan, stream, and `GEOADD` replies and requests are built with exact-size arrays instead of LINQ or closures.
- A `GET` sent on a socket that closed before any reply byte is sent once more on a new socket, as `ExecuteAsync` already did. Before, the fast `GET` path threw instead.

## 0.2.7

- The library is AOT-compatible and trimmable. `Ruvio.Client.Tour` publishes a native binary.
- `GET`, `INCR`, and `GEOADD` on a standalone connection write the command name from a generated UTF-8 span.

## 0.2.6

- A dropped command or shard socket is opened again before the next command. The command that sees the drop fails. A single `GET`, `MGET`, `EXISTS`, `HGET`, `TTL`, or `PTTL` is sent once more when no reply byte arrived. Pipelines are not retried. Pub/Sub channels are subscribed again on the new socket.
- `ConnectionLost` and `ConnectionRestored` (`RuvioConnectionNotice`: host, port, subscriber, error). `ConnectionFailed` still fires on the drop.
- `RuvioClientOptions.Reconnect` (default true), `MaxReconnectAttempts`, `ReconnectBaseDelay`, `ReconnectMaxDelay`, and `ReconnectDelay`. `ReconnectDelay` returning null stops the retry and ignores `MaxReconnectAttempts`. `ReconnectAsync` opens the socket when automatic retry is off.

## 0.2.5

- Pub/Sub uses a dedicated socket. `SubscribeAsync(channel, handler)`, `Message`, and `ListenAsync` deliver payloads without a manual read loop on the command connection.
- `IsConnected` and `ConnectionFailed`.

## 0.2.4

- `DeleteAsync(key)` sends `DEL` for one key. The list overload remains for many keys.
- `PopListLeftBlockingAsync(key, timeout)` and `PopListRightBlockingAsync(key, timeout)` send `BLPOP` / `BRPOP` without a key list.

## 0.2.3

- A `MOVED` reply after a plain connect (or with discovery off) learns the slot map and retries, so `SET` on the seed port no longer throws.

## 0.2.2

- A sharded server is discovered from one seed address. Commands are routed by hash slot, `MOVED` is followed, and `MGET` / `MSET` / `DEL` split across shards.
- `GetClusterShardsAsync`, `GetClusterInfoAsync`, `GetClusterMyIdAsync`, `GetClusterKeySlotAsync`, `ReadOnlyAsync`, `ReadWriteAsync`.
- `GetHashSlot`, `IsCluster`, `ShardCount`, and `RuvioClientOptions.DiscoverCluster`.

## 0.2.1

- `AddSortedSetAsync(key, ("ada", 100), ("bob", 50))` takes `(member, score)` tuples, with or without `SortedSetAddFlags`.
- `AddSortedSetAsync(key, member, score)` adds one member and returns whether it was new.

## 0.2.0

- `ConnectAsync(host, port, database)`, `ConnectAsync(IPAddress, port, database)`, `ConnectAsync(IPEndPoint, database)`, and `RuvioClientOptions.Database`.
- `RuvioConnection.GetDatabaseAsync`, `FlushDatabaseAsync`, `FlushAllDatabasesAsync`, `SwapDatabasesAsync`, `MoveKeyAsync`.
- The option overloads in the table above, and `PingAsync(message)`.
- Breaking: `LoadFunctionAsync(body)` became `LoadFunctionAsync(name, body)`. The old form sent a request Ruvio always refused. `DeleteFunctionAsync` returns `Task<bool>`.
