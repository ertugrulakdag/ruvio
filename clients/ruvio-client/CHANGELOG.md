# Changelog

Notable changes to the Rust client crate `ruvio-client`. New entries go under "Unreleased"; `scripts/release_version.py` moves them under the version number when the package is released.

## Unreleased

- `Client::sharded_subscriber(channel)`: a Pub/Sub socket to the shard that owns a sharded channel, for `ssubscribe` on a cluster (a `subscriber()` socket reaches one shard only, so `SSUBSCRIBE` of a channel on another shard got `MOVED`).
- Cluster routing is cheaper: commands no longer allocate on the way to their node (no upper-cased name, key list or `host:port` string per command), connections are found by node index, and a pipeline spread over several nodes writes every node before reading any, so it costs one round trip instead of one per node. A node that fails mid-pipeline no longer leaves the other nodes' sockets out of step. On a 4-shard local server cluster user CPU per pipelined request fell 3.6-21×; see `docs/evidence/2026-10-16-client-cluster-routing.md`.
- **Behaviour change:** `RENAME` / `RENAMENX` whose keys hash to different slots now fail with
  `Error::Server("CROSSSLOT Keys in request do not hash to the same slot")` before anything is
  sent, instead of being emulated with `TYPE`, a copy, `PEXPIRE` and `DEL`. Put both keys under one
  hash tag.
- **Behaviour change:** any other command whose keys span slots and that is not split (`COPY`,
  `LMOVE`, `SMOVE`, `SINTERSTORE`, `BITOP`, `EVAL`, ...) now fails with the same `CROSSSLOT` error
  before sending, instead of going to the first key's shard.
- **Behaviour change:** in `execute_many`, `MGET` / `MSET` / `DEL` / `EXISTS` / `UNLINK` / `TOUCH`
  with keys in several slots are split per slot like the plain command, and the merged reply sits
  at the command's index (they used to go whole to the first key's shard). Any other cross-slot
  command gets the `CROSSSLOT` error at its index; the rest of the pipeline still runs.
- A `MOVED` reply now points that slot at the named node and sends the command again there. The
  whole slot map is reloaded only when the node is unknown to the client, or on the command's
  second and third `MOVED` (it used to be reloaded on every `MOVED`). The slot keeps pointing at the
  named node even when the reloaded map names another owner. Still at most 3 redirects per
  command; the 4th `MOVED` is returned.
- The cluster client now takes each command's key positions from the server's command table
  (`src/command_keys.rs`, generated from `clients/testdata/commands.json`) instead of its own
  hand-kept lists, and is checked against `clients/testdata/key_vectors.json`. This fixes routing
  for `GEORADIUS` and `GEORADIUSBYMEMBER` with `STORE`/`STOREDIST` (the destination key was
  ignored) and `GEOSEARCHSTORE` (the source key was ignored).

## 0.2.6 to 0.3.1

These notes were recorded together and shipped over these versions.

- Typed methods for every matrix command (287/287): `DELEX`, `SET ... IFEQ`, `LPUSHCAP`/`RPUSHCAP`,
  the full `BF.*` and `JSON.*` families including `JSON.INDEX`/`JSON.FIND`, bitmaps, HyperLogLog,
  `SSCAN`/`ZSCAN`, `ZUNION`/`ZINTER`/`ZDIFF` and their stores, `DUMP`/`RESTORE`, `WAIT`, `COMMAND`,
  `LATENCY`, and the rest of the strings, sets, sorted sets and hashes.
- Helpers: `LockGuard`, `LeaseGuard`, `SemaphorePermit`, `rate_limit`, `get_or_create`,
  `idempotent`, `scan_iter`/`hscan_iter`/`sscan_iter`/`zscan_iter`, `push_recent`/`recent`,
  `seen_before`/`maybe_seen`.
- `execute_binary`, `set_bytes`, `hash_slot_bytes`, `RespValue::into_string` / `into_bytes`.
- Fewer allocations on existing commands: fixed-arity commands no longer build `Vec<String>`.

## 0.2.5

- `geo_add`, `geo_dist`, `geo_hash`, `geo_pos`, `geo_search`, `geo_search_store`, `geo_radius`, and `geo_radius_by_member`.

## 0.2.4

- A dropped command or shard socket is opened again before the next command. The command that sees the drop returns an error. Subscriptions on a dedicated Pub/Sub socket are sent again after it reopens.
- `on_connection_lost` and `on_connection_restored` take a `ConnectionNotice` (`host`, `port`, `subscriber`).
- `ClientOptions::reconnect` (default true), `max_reconnect_attempts`, `reconnect_base_delay`, `reconnect_max_delay`, `set_reconnect_delay`, and `reconnect`. `None` from the delay stops the retry. `max_reconnect_attempts` is not applied while a custom delay is set.

## 0.2.3

- `subscribe` / `psubscribe` / `ssubscribe` open a dedicated Pub/Sub socket so command traffic stays on the original connection. `subscriber()` still returns that extra socket if you want it separately. `listen` calls a callback for each delivery.

## 0.2.2

- `del_key`, `unlink_key`, `exists_key`, `watch_key`, `blpop_key`, and `brpop_key` take one key. The slice methods remain for many keys.

## 0.2.1

- A sharded server is discovered from one seed address. Commands are routed by hash slot, `MOVED` is followed, and `MGET` / `MSET` / `DEL` split across shards.
- `cluster_shards`, `cluster_info`, `cluster_myid`, `cluster_keyslot`, `readonly`, `readwrite`.
- `hash_slot`, `is_cluster`, `shard_count`, and `ClientOptions::discover_cluster`.

## 0.2.0

- Typed methods for every command Ruvio supports.
- `ClientOptions::database`, `connect_database`, `connect_ip`, `connect_addr`, `select`, `into_database`, and `database`.
- `set_expires_at` (`PXAT`), `set_keep_ttl` (`KEEPTTL`), `set_and_get` (`GET`), and `ping_message`.
- `expire` sends `PEXPIRE` when the duration is not whole seconds. It used to round up to the next second.
- `RespValue::as_integer`, `as_array`, and `is_null`.
- The minimum Rust version is 1.87.
