# Integrations

Packages that connect Ruvio to a framework. They are separate from the server and the clients, depend on the client, and are MIT licensed ([license](LICENSE)).

| Folder | Language | Contents |
| --- | --- | --- |
| [`dotnet/`](dotnet/README.md) | C# | `IDistributedCache`, ASP.NET Core output caching, Data Protection key-ring storage, rate limiting, HTTP idempotency and distributed locks, each its own NuGet package |

Java, Python and Node.js integrations would live in their own language folders. A server that no integration uses stores no extra state.
