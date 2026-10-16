<p align="center">
  <a href="https://ruviodb.com/"><img src="../docs/images/ruvio-mark.png" width="88" alt="Ruvio"></a>
</p>

<h1 align="center">Ruvio clients</h1>

<p align="center">First-party application clients for six languages. They speak RESP2, never send <code>CLIENT SETINFO</code>, cover every command in <code>docs/compatibility-matrix.json</code> and discover a sharded server from one seed.</p>

<p align="center">
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

<p align="center">
  <a href="https://ruviodb.com/docs/clients/">Clients guide</a> ·
  <a href="https://ruviodb.com/get-started/">Packages</a> ·
  <a href="https://github.com/salihcantekin/ruvio/blob/main/integrations/README.md">Integrations</a> ·
  <a href="https://github.com/salihcantekin/ruvio">Ruvio server</a>
</p>

---

## Choose a client

| Language | Package | Install | Tests |
| --- | --- | --- | --- |
| Rust | [`ruvio-client`](ruvio-client/README.md) | `cargo add ruvio-client` | `cargo test --manifest-path clients/ruvio-client/Cargo.toml` |
| C# | [`Ruvio.Client`](csharp/Ruvio.Client/README.md) | `dotnet add package Ruvio.Client` | `dotnet test` on `Ruvio.Client.Tests` and `Ruvio.Client.AspNetCore.Tests` |
| Go | [`clients/go`](go/README.md) | `go get github.com/salihcantekin/ruvio/clients/go` | `go test ./...` |
| Java | [`ruvio-client`](java/README.md) | `io.github.salihcantekin:ruvio-client` (Maven Central) | `mvn -B test` |
| Python | [`ruvio-client`](python/README.md) | `pip install ruvio-client` | `python -m pytest` |
| Node.js | [`ruvio-client`](node/README.md) | `npm install ruvio-client` | `npm test` |

Also here: [`ruvio-ui`](ruvio-ui/README.md), the browser operator UI (AGPL-3.0). The clients are MIT licensed ([license](LICENSE)).

Live tests skip unless `RUVIO_TEST_ADDR` is set; cluster tests need `RUVIO_CLUSTER_ADDR` (a server started with `shards = 3` and `port_mode = "per-shard"`). Details are in each client's README.

## C# and .NET

`Ruvio.Client.AspNetCore` registers one `RuvioClient` as `RuvioClient`, `IRuvioClient` (commands), and `IRuvioConnection` (reconnect and dispose).

Separately packaged [.NET integrations](../integrations/dotnet/README.md) add `IDistributedCache`, ASP.NET Core output caching, Data Protection key-ring storage and opt-in HTTP rate limiting and idempotency. They depend on the client; the client does not depend on framework integrations.

## Run the client suites

```bash
cargo build --bin ruvio
RUVIO_ADDR=127.0.0.1:16382 RUVIO_DATA_DIR=/tmp/ruvio-clients ./target/debug/ruvio
python3 scripts/run_client_suites.py --port 16382
```

The script runs the Rust and C# unit tests. Set `RUVIO_TEST_ADDR` (the script does this) so `clients/ruvio-client/tests/live.rs` also talks to that process.

## Connection behavior (Rust and C#)

- Pub/Sub uses a **second TCP socket**. `GET`, `SET`, `PING`, and `PUBLISH` stay on the command connection.
- A dropped socket, including a shard socket, is opened again before the next command. The command that sees the drop fails.
- The C# client sends one `GET`, `MGET`, `EXISTS`, `HGET`, `TTL`, or `PTTL` again when the socket closes before any reply byte; a pipeline is not retried. The Rust client does not retry that command.
- Subscriptions are sent again on the new Pub/Sub socket.
- Retry is on by default. Set `Reconnect` / `reconnect` to false and call `ReconnectAsync` / `reconnect`, or pass a delay function. Null / `None` stops the retry.
- Connection lost and restored events carry the host, port, and whether the socket is the subscriber. ASP.NET Core binds the retry settings and can attach those events when the singleton opens.

## Tours

A tour of nearly every method, against a local server:

```bash
cargo run --manifest-path clients/ruvio-client/Cargo.toml --example tour -- 127.0.0.1 6379
dotnet run --project clients/csharp/Ruvio.Client.Tour -- 127.0.0.1 6379
```

> [!WARNING]
> On a standalone server the tour uses database 15. The C# tour finishes with `FLUSHALL`. Do not point either program at a server you need to keep.
