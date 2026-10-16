# Logical Databases

Ruvio has numbered logical databases, like Redis and Garnet with cluster mode
off. A connection starts on database 0. `SELECT n` switches that connection.
`user:1` in DB 0 and `user:1` in DB 1 are different keys.

The count is `databases` (default 16, allowed 1–256):

```toml
[protocol]
databases = 16
```

`RUVIO_DATABASES` overrides the file. `CONFIG GET databases` reports it.

## Commands

| Command | Behaviour |
| --- | --- |
| `SELECT n` | Connection-local. `ERR DB index is out of range` outside `0..databases`. |
| `FLUSHDB [ASYNC\|SYNC]` | Empties the selected database on every shard. O(keys in that database). `ASYNC` runs synchronously. |
| `FLUSHALL [ASYNC\|SYNC]` | Empties every database. |
| `SWAPDB a b` | Exchanges two databases in O(1). Connections on `a` see `b`'s keys. |
| `MOVE key db` | `1` when moved. `0` when the key is missing or already exists in `db`. Same database is an error. |
| `DBSIZE` | O(1) count of the selected database, keys past their TTL included until reclaimed (as in Redis). |
| `SCAN` | Only the selected database. |
| `INFO` | Adds `db<N>:keys=..,expires=..,avg_ttl=..` for each non-empty database. |

Every other key command, `MULTI`/`EXEC`, `WATCH`, `EVAL`/`FCALL`, and blocking
commands (`BLPOP`, `XREAD ... BLOCK`) act on the selected database. Replies
that name a key (`BLPOP`, `XREAD`) return the user key.

## Storage

Each shard holds one map per database, as Garnet holds one store per
database. A map is created on first use. Keys are stored unchanged; the
database is the map, not a key prefix. The shard has one active map, and the
engine switches it before each command, so the key path is the same code for
every database. Memory limits, eviction, and active expiry are shard-wide and
sample every database.

Routing hashes the user key, so `{tag}` keys stay on one shard across
databases.

Persistence keeps one WAL per shard:

- Each WAL record carries its database in the header flags field. Database 0
  leaves it zero, so database 0 records are byte-identical to volumes written
  before databases existed. No record grows, and there is no extra write or
  fsync.
- `FLUSHDB`, `FLUSHALL`, `SWAPDB`, and `MOVE` are each one record, not one
  record per key.
- Checkpoint entries carry the database in a field that was zero before, so
  old checkpoints load as database 0.
- Replication frames are WAL records, so replicas apply each write in its
  database.

A binary older than this format refuses a volume that holds data outside
database 0 rather than loading it into database 0.

## Compared with Redis and Garnet

| | Redis | Garnet | Ruvio |
| --- | --- | --- | --- |
| Default count | 16 (`databases`) | 16 (`MaxDatabases`) | 16 (`databases`) |
| Upper bound | configurable | 256 | 256 |
| `SELECT` | yes | yes | yes |
| `FLUSHDB` / `FLUSHALL` | yes | yes | yes, `ASYNC` is synchronous |
| `SWAPDB` | yes | yes | yes |
| `MOVE` | yes | yes | yes |
| `INFO keyspace` lines | yes | yes | yes |
| Keyspace channel `__keyspace@<db>__` | yes | yes | yes |
| Pub/Sub ignores `SELECT` | yes | yes | yes |
| ACL is process-wide across databases | yes | yes | yes |
| Cluster mode | DB 0 only | DB 0 only | DB 0 only (`shards > 1`) |
| Durable per database | yes | yes | yes |

Differences from Redis:

- `SWAPDB` does not wake a client blocked in `BLPOP`/`XREAD` whose key now
  exists in its database. The client wakes on the next push to that key.
- Scripts cannot call `SELECT`, `FLUSHDB`, `FLUSHALL`, `SWAPDB`, or `MOVE`.
  Redis allows `SELECT` inside a script.
- `COPY key dest DB n` is absent because Ruvio has no `COPY`.

## Ruvio-specific rules

- **Hot keys are DB 0 only.** `hot_keys` patterns route to the lock-free hot
  store only on database 0. The same key name in another database uses the
  normal store and every command works on it.
- **Cluster mode.** With `shards > 1`, `SELECT n` for n ≠ 0, `SWAPDB`, and
  `MOVE` return an error, as in Redis Cluster.
- **Hot keys and `SWAPDB`/`MOVE`.** Hot keys stay in the hot store of
  database 0. `SWAPDB 0 n` and `MOVE` do not carry them.
- **Client tracking** invalidates per database and key. `FLUSHDB`, `FLUSHALL`,
  and `SWAPDB` invalidate every tracked key on the connection.

## Client

`Ruvio.Client`:

```csharp
await using var connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions { Database = 2 });
var ruvio = await connection.GetDefaultDatabaseAsync();
ruvio = await connection.GetDatabaseAsync(3); // SELECT 3 on the same socket
bool moved = await ruvio.MoveKeyAsync("user:1", 4);
await ruvio.SwapDatabasesAsync(3, 4);
await ruvio.FlushDatabaseAsync();
```

`GetDatabaseAsync(n)` sends `SELECT n` on the same socket and returns the
same client. It is not a second connection: every later command on that
client uses database `n`.
