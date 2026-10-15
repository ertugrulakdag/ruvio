# C# client and ASP.NET Core registration

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

await using var db = await RuvioClient.ConnectAsync("127.0.0.1", 6379);
await db.SetAsync("session:ada", "hello");
string? value = await db.GetStringAsync("session:ada");
```

Run the tests from this folder with `dotnet test`. Start a local server first and set `RUVIO_TEST_ADDR=127.0.0.1:6379` to include the live tests. The client is MIT licensed ([license](../LICENSE)). The [.NET integrations](../../integrations/dotnet/README.md) build on it.
