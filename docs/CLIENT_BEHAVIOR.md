[Ruvio](../README.md) / [Docs](README.md) / Client behavior

# Client behavior

What the six clients do when a cluster redirects, a command spans slots, or a socket drops. They agree on most of it; the rows marked **differs** are known differences, kept as they are until someone decides to align them. Update this page in the same change as the client code. File references are relative to `clients/`.

## Shared test data

| File | Checked by |
| --- | --- |
| `testdata/slots.json` (key to hash slot, with hash tags and non-ASCII keys) | the server (`src/routing.rs`) and one test per client (`slot_vectors`) |
| `../docs/compatibility-matrix.json` (every command) | each client's coverage test |
| `testdata/commands.json` (arity, flags and key rule per command) | the server (`src/protocol/client_spec.rs`); `scripts/gen_client_keys.py` turns it into each client's key table |
| `testdata/key_vectors.json` (commands with the keys the server routes them by) | the server and one test per client (`key_vectors`) |

## Same in every client

| Topic | Behavior |
| --- | --- |
| Commands split across slots | `MGET`, `MSET`, `DEL`, `EXISTS`, `UNLINK` and `TOUCH` with keys in several slots are sent per slot and the replies merged (`MGET` keeps key order, counts are summed) |
| `ASK` | Not followed; no client sends `ASKING`. Ruvio has no online slot migration, so the server does not send `ASK` |
| Reconnect | A dropped socket is opened again on the next command |
| Pipelines in a cluster | Split per node, replies returned in the original order; a transaction must stay in one slot |

## Differences

| Topic | Rust | C# | Go | Java | Node | Python |
| --- | --- | --- | --- | --- | --- | --- |
| **differs**: `MOVED` redirects followed per command | 3 | 3 | 1 | 1 | 1 | 1 |
| **differs**: what a `MOVED` reloads | the whole slot map, every time | the whole slot map, every time | that slot; the whole map only for an unknown node or a second `MOVED` | same as Go | same as Go | same as Go |
| **differs**: `RENAME`/`RENAMENX` across slots | emulated (`TYPE`, copy, `PEXPIRE`, `DEL`); fails for value types it cannot copy | emulated, same as Rust | refused before sending (`ErrCrossSlot`) | refused before sending (`RuvioServerException` "CROSSSLOT ... checked by the client") | refused before sending (`RuvioError`, code `CROSSSLOT`) | refused before sending (`CrossSlotError`) |
| **differs**: other two-key commands across slots (`COPY`, `LMOVE`, `SMOVE`, ...) | sent to the first key's shard; the server answers | same as Rust | refused before sending | refused before sending | refused before sending | refused before sending |
| **differs**: cross-slot `MGET`/`DEL` inside a pipeline | sent whole to the first key's shard | same as Rust | the whole pipeline is refused | split like a plain command | error at that index only | the pipeline is refused before sending |
| **differs**: a command whose socket dropped before any reply byte | never sent again | sent once more for `GET`, `MGET`, `EXISTS`, `HGET`, `TTL`, `PTTL` when `Reconnect` is on; never for pipelines or the binary API | never | never | never | never |
| **differs**: reconnect option | `ClientOptions::reconnect` (default on), attempts and backoff settings | `RuvioClientOptions.Reconnect` (default on), attempts and backoff settings | always on, no option | always on, no option | `reconnect` (default on) | always on, no option |

## Where it lives

| Client | Routing and redirects | Key rules (interpreter, and the generated table) |
| --- | --- | --- |
| Rust | `ruvio-client/src/cluster.rs` | `ruvio-client/src/cluster.rs`, `command_keys.rs` |
| C# | `csharp/Ruvio.Client/Cluster/ClusterRouter.cs` | `csharp/Ruvio.Client/Cluster/ClusterKeys.cs`, `CommandKeyRules.g.cs` |
| Go | `go/cluster.go`, `go/cluster_route.go`, `go/cluster_pipeline.go` | `go/cluster_keys.go`, `command_keys_gen.go` |
| Java | `java/.../internal/ClusterRouter.java`, `RoutedRequest.java` | `java/.../internal/ClusterKeys.java`, `CommandKeyRules.java` |
| Node | `node/src/cluster.ts` | `node/src/cluster.ts`, `command-keys.ts` |
| Python | `python/ruvio/cluster.py`, `python/ruvio/asyncio/cluster.py` | `python/ruvio/_cluster.py`, `_command_keys.py` |
