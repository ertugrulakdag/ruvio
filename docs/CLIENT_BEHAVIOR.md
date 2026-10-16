[Ruvio](../README.md) / [Docs](README.md) / Client behavior

# Client behavior

What the six clients do when a cluster redirects, a command spans slots, or a socket drops. They agree on most of it; the rows marked **differs** are the known differences left. Update this page in the same change as the client code. File references are relative to `clients/`.

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
| Commands split across slots | `MGET`, `MSET`, `DEL`, `EXISTS`, `UNLINK` and `TOUCH` with keys in several slots are sent per slot and the replies merged (`MGET` keeps key order, counts are summed, `MSET` answers `OK`). Not atomic |
| Split commands inside a pipeline | Split the same way; the merged reply sits at the command's index and the other replies keep their order. A transaction must stay in one slot |
| Other commands across slots | `RENAME`, `RENAMENX`, `COPY`, `LMOVE`, `SMOVE`, `SINTERSTORE`, `BITOP`, `EVAL` and every other command that is not split fail with `CROSSSLOT` before anything is sent; nothing is emulated |
| `MOVED` | The slot is pointed at the named node and the command is sent there, at most 3 times; a fourth `MOVED` is returned. The whole slot map is reloaded only when the named node is unknown, or from the command's second `MOVED` on |
| `ASK` | Not followed; no client sends `ASKING`. Ruvio has no online slot migration, so the server does not send `ASK` |
| Socket dropped before any reply byte | The command fails and is never sent again, reads included. The socket is opened again for the next command |
| Pipelines in a cluster | Grouped per node, replies returned in the original order |

## Differences

| Topic | Rust | C# | Go | Java | Node | Python |
| --- | --- | --- | --- | --- | --- | --- |
| **differs**: a non-split command whose keys span slots, inside a pipeline | `CROSSSLOT` error at its index; the rest runs | sent to the first key's shard; the server answers | the whole pipeline fails with `ErrCrossSlot` before sending | `CROSSSLOT` error at its index; the rest runs | `CROSSSLOT` error at its index; the rest runs | the pipeline is refused before sending |
| **differs**: the `CROSSSLOT` error | `Error::Server` with the server's text | `RuvioException`, text ends "(checked by the client, nothing was sent)" | `ErrCrossSlot` | `RuvioServerException`, text ends "(checked by the client, nothing was sent)" | `RuvioError`, code `CROSSSLOT` | `CrossSlotError` |
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
