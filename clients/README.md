# First-party clients

Ruvio ships application clients for six languages. They speak RESP2, never send
`CLIENT SETINFO`, cover every command in `docs/compatibility-matrix.json`, and
discover a sharded server from one seed.

| Language | Crate / package | Tests |
| --- | --- | --- |
| Rust | `clients/ruvio-client` (`ruvio-client`) | `cargo test --manifest-path clients/ruvio-client/Cargo.toml` |
| C# | `clients/csharp` (`Ruvio.Client`) | `dotnet test` on `Ruvio.Client.Tests` and `Ruvio.Client.AspNetCore.Tests` |
| Go | `clients/go` | `go test ./...` |
| Java | `clients/java` | `mvn -B test` |
| Python | `clients/python` | `python -m pytest` |
| Node.js | `clients/node` | `npm test` |

Live tests skip unless `RUVIO_TEST_ADDR` is set; cluster tests need `RUVIO_CLUSTER_ADDR` (a
server started with `shards = 3` and `port_mode = "per-shard"`). Details are in each client's README.

`Ruvio.Client.AspNetCore` registers one `RuvioClient` as `RuvioClient`, `IRuvioClient` (commands), and `IRuvioConnection` (reconnect and dispose).

Separately packaged [.NET integrations](../integrations/dotnet/README.md) add
`IDistributedCache`, ASP.NET Core output caching, Data Protection key-ring storage
and opt-in HTTP rate limiting and idempotency. They depend on
the client; the client does not depend on framework integrations.

```bash
cargo build --bin ruvio
RUVIO_ADDR=127.0.0.1:16382 RUVIO_DATA_DIR=/tmp/ruvio-clients ./target/debug/ruvio
python3 scripts/run_client_suites.py --port 16382
```

The script runs the Rust and C# unit tests. Set `RUVIO_TEST_ADDR` (the
script does this) so `clients/ruvio-client/tests/live.rs` also talks to
that process.

Pub/Sub on both clients uses a **second TCP socket**. `GET`, `SET`,
`PING`, and `PUBLISH` stay on the command connection.

A dropped socket, including a shard socket, is opened again before the
next command. The command that sees the drop fails. The C# client sends
one `GET`, `MGET`, `EXISTS`, `HGET`, `TTL`, or `PTTL` again when the
socket closes before any reply byte; a pipeline is not retried. The Rust
client does not retry that command. Subscriptions are
sent again on the new Pub/Sub socket. Retry is on by default. Set
`Reconnect` / `reconnect` to false and call `ReconnectAsync` / `reconnect`,
or pass a delay function. Null / `None` stops the retry. Connection lost
and restored events carry the host, port, and whether the socket is the
subscriber. ASP.NET Core binds the retry settings and can attach those
events when the singleton opens.

A tour of nearly every method, against a local server:

```bash
cargo run --manifest-path clients/ruvio-client/Cargo.toml --example tour -- 127.0.0.1 6379
dotnet run --project clients/csharp/Ruvio.Client.Tour -- 127.0.0.1 6379
```

On a standalone server the tour uses database 15. The C# tour finishes
with `FLUSHALL`. Do not point either program at a server you need to keep.
