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
| `testdata/resp.json` (RESP wire vectors: `encode` maps a command's `args`, or `args_hex` for raw bytes, to the exact request bytes; `decode` maps reply bytes to a value `{"simple"}`, `{"error"}`, `{"int"}` (a decimal string beyond 2^53), `{"bulk"}`/`{"bulk_hex"}`, `{"null": true}` or `{"array": [...]}`; `incomplete` lists replies cut short, where the parser must wait for more bytes. Wire bytes are `bytes` (UTF-8 text) or `bytes_hex`. RESP2 only, because the Rust and C# clients do not parse RESP3) | one test per client (`resp_vectors`): every encoder and parser, Python with its own parser and hiredis when installed |
| `testdata/command_methods.json` (every matrix command to the method that sends it in each client: `{"commands": {"GET": {"rust": ["get", "get_string"], "csharp": "GetStringAsync", ...}}}`, sorted by command, one row per command; a field is one method name or a list when several methods send the command; a `Type.` prefix (Go `Tx.`/`PubSub.`, Node `Subscriber.`/`Transaction.`, Python `PubSub.`, C# `RuvioConnection.`) or Java `Class#` names a method off the main client) | each client's coverage test |

## Same in every client

| Topic | Behavior |
| --- | --- |
| Commands split across slots | `MGET`, `MSET`, `DEL`, `EXISTS`, `UNLINK` and `TOUCH` with keys in several slots are sent per slot and the replies merged (`MGET` keeps key order, counts are summed, `MSET` answers `OK`). Not atomic |
| Split commands inside a pipeline | Split the same way; the merged reply sits at the command's index and the other replies keep their order. A transaction must stay in one slot |
| Other commands across slots | `RENAME`, `RENAMENX`, `COPY`, `LMOVE`, `SMOVE`, `SINTERSTORE`, `BITOP`, `EVAL` and every other command that is not split fail with `CROSSSLOT` before anything is sent; nothing is emulated |
| `MOVED` | The slot is pointed at the named node and the command is sent there, at most 3 times; a fourth `MOVED` is returned. The whole slot map is reloaded only when the named node is unknown, or from the command's second `MOVED` on. Ruvio forwards a misrouted key instead of answering `MOVED` (only a sharded Pub/Sub channel is redirected), so against Ruvio this path is exercised by the clients' scripted fake-server tests, not live |
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

## Routing cost

A cluster client pays for routing on every command, so the routing path follows the same shape in every client. The goal: per request, at most 1.3× the user-space CPU of the plain single-connection path at the same pipeline depth, and no allocation per command once the client is warm. Splitting a pipeline over N nodes also means N writes and N reads instead of one; that system-call cost is inherent and is not counted against the goal.

| Step | Rule |
| --- | --- |
| Topology | A node list plus a 16384-entry table of node indexes, replaced as a whole on refresh. A node's connection is found by its index, never by building a `host:port` string per command |
| Command name and keys | Found from the caller's arguments (or the already encoded frame) without allocating: a case-insensitive compare against the key table, and a direct path for the common "first argument is the key" rule |
| Slot | Hash tag and CRC16 computed in place over the key bytes |
| One command | After the connection is picked, the same send path as the plain client. Nothing is copied in case a `MOVED` arrives; a redirect re-sends from the caller's arguments |
| Replies | Only error replies are checked for `MOVED`, by prefix |
| Pipelines | One pass stores a node index per command in a reused buffer. If every command goes to one node, the plain pipeline path runs unchanged. Otherwise each command is encoded once, straight into its node's write buffer; every node is written before any is read (or the nodes run concurrently), and each reply is stored at its command's index |
| Split commands and transactions | Keep their own path (`MGET`/`MSET`/... split per slot, a transaction pinned to one node); the rules above are for ordinary commands |

Before and after numbers per client: [evidence/2026-10-16-client-cluster-routing.md](evidence/2026-10-16-client-cluster-routing.md). Each client's benchmark tool has the same plain, cluster and one-node rows (`cluster_cost` example, `--cluster-cost`, `RoutingCostBench`, `bench:cluster-cost`, `bench.py --cluster-cost`, Go `TestClusterCost`).
