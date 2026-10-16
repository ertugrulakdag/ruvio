<p align="center">
  <a href="https://ruviodb.com/"><img src="../../docs/images/ruvio-mark.png" width="88" alt="Ruvio"></a>
</p>

<h1 align="center">C# client and ASP.NET Core registration</h1>

<p align="center">The .NET projects under <code>clients/csharp</code>: the <a href="Ruvio.Client/README.md">Ruvio.Client</a> SDK and its ASP.NET Core registration.</p>

<p align="center">
  <a href="https://www.nuget.org/packages/Ruvio.Client"><img alt="NuGet" src="https://img.shields.io/nuget/v/Ruvio.Client?label=nuget"></a>
  <a href="https://www.nuget.org/packages/Ruvio.Client"><img alt="Downloads" src="https://img.shields.io/nuget/dt/Ruvio.Client?label=downloads"></a>
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

<p align="center">
  <a href="https://ruviodb.com/docs/clients/csharp/">C# guide</a> ·
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/README.md">All clients</a> ·
  <a href="https://github.com/salihcantekin/ruvio/blob/main/integrations/dotnet/README.md">Integrations</a> ·
  <a href="https://github.com/salihcantekin/ruvio">Ruvio server</a>
</p>

---

| Project | NuGet package | What it is |
| --- | --- | --- |
| [`Ruvio.Client`](Ruvio.Client/README.md) | `Ruvio.Client` | RESP2 client with typed methods for every server command, single-connection multiplexing and cluster routing |
| [`Ruvio.Client.AspNetCore`](Ruvio.Client.AspNetCore/README.md) | `Ruvio.Client.AspNetCore` | Dependency-injection registration, configuration binding and an opt-in `PING` health check |
| `Ruvio.Client.Generators` | none | Source generator that emits the verb encoders used by `Ruvio.Client` |
| `Ruvio.Client.Tests`, `Ruvio.Client.AspNetCore.Tests` | none | xUnit tests; live tests run only when `RUVIO_TEST_ADDR` is set |
| [`Ruvio.Client.Benchmarks`](Ruvio.Client.Benchmarks/README.md) | none | BenchmarkDotNet comparisons |
| `Ruvio.Client.Tour` | none | Calls nearly every client method against a local server (it selects database 15 and flushes it) |

```bash
dotnet add package Ruvio.Client
```

```csharp
using Ruvio.Client;

await using var connection = await RuvioConnection.ConnectAsync("127.0.0.1", 6379);
var db = await connection.GetDefaultDatabaseAsync();
await db.SetAsync("session:ada", "hello");
string? value = await db.GetStringAsync("session:ada");
```

Run the tests from this folder with `dotnet test`. Start a local server first and set `RUVIO_TEST_ADDR=127.0.0.1:6379` to include the live tests. The client is MIT licensed ([license](../LICENSE)). The [.NET integrations](../../integrations/dotnet/README.md) build on it.
