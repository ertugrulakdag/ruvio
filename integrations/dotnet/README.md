<p align="center">
  <a href="https://salihcantekin.github.io/ruvio/"><img src="https://raw.githubusercontent.com/salihcantekin/ruvio/main/site/images/ruvio-mark.png" width="88" alt="Ruvio"></a>
</p>

<h1 align="center">Ruvio .NET integrations</h1>

<p align="center">Separately packaged NuGet packages that connect <a href="https://salihcantekin.github.io/ruvio/">Ruvio</a> to ASP.NET Core and Microsoft.Extensions.</p>

<p align="center">
  <a href="https://github.com/salihcantekin/ruvio/blob/main/integrations/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

<p align="center">
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/csharp/Ruvio.Client/README.md">Ruvio.Client</a> ·
  <a href="https://salihcantekin.github.io/ruvio/testing-integrations.html">Integrations tests</a> ·
  <a href="https://github.com/salihcantekin/ruvio">Ruvio server</a>
</p>

---

These are independently packaged .NET integrations, not server features or
language-neutral libraries. Java, Python and Node integrations belong in their
own language directories and package registries. The server and existing clients
keep their current paths.

| NuGet package | Responsibility |
| --- | --- |
| `Ruvio.Extensions.Caching` | `IDistributedCache` for web, worker and console applications |
| `Ruvio.AspNetCore.RateLimiting` | Endpoint-opt-in distributed fixed-window budgets |
| `Ruvio.AspNetCore.Idempotency` | Endpoint-opt-in bounded HTTP request deduplication and response replay |
| `Ruvio.AspNetCore.OutputCaching` | Native ASP.NET Core output-cache store with distributed tag invalidation |
| `Ruvio.AspNetCore.DataProtection` | Encrypted key-ring persistence for shared cookies and protected payloads |
| `Ruvio.AspNetCore.DistributedLock` | Distributed locks with owner-checked release and automatic renewal, plus a `[RuvioLock]` endpoint attribute (409 when taken, request cancelled on lock loss, metrics) |

Use the existing `Ruvio.Client.AspNetCore` package for client DI/configuration and
opt-in PING health checks. `IRuvioClient` is the shared command dependency;
`IRuvioConnection` owns connection lifecycle. Do not create a connection per HTTP
request or change the shared client's selected database while integrations use it.
Each integration neither owns nor disposes the injected client.

## Composition

The caching provider references Microsoft.Extensions abstractions, not the ASP.NET
Core shared framework. It can serve ASP.NET Core Session and the distributed tier
of HybridCache. This does not automatically provide cross-instance local-cache
invalidation. Existing Redis provider cache entries have a different storage
format; use a separate prefix and warm the cache rather than treating them as
interchangeable.

Use `Ruvio.AspNetCore.OutputCaching` for `IOutputCacheStore`, not the distributed
cache adapter. It preserves framework output-cache policies and adds O(1) tag
eviction without scans. Atomic tag publication deliberately confines each
`InstanceName` to one hash slot; this is not full-cluster scaling for one namespace.
See its README for generation/expiry races and buffering limits.

Data Protection and Session have different storage requirements: the distributed
cache stores expiring session contents; Data Protection retains the encryption
key ring without TTL. The latter requires an encryptor by default and must use
persistent, backed-up, non-evicting storage. Its connection-factory overload can select
a separate durable Ruvio connection. Sharing the ring and `SetApplicationName`
allows instances to decrypt each other's session/authentication cookies. The
Data Protection package includes a Session composition example, exercised across
two real HTTP hosts. Neither adapter changes Session's last-writer-wins semantics.

The HTTP packages explicitly depend on ASP.NET Core. Register authentication and
authorization before them, and apply metadata only to endpoints that need the
feature. See each package's README for its full API and limits.

Choose middleware order deliberately. Rate limiting before idempotency charges
retries/replays too; idempotency before rate limiting can retain the downstream
response according to its replay policy. Do not silently assume these orders are
equivalent. Put the application's exception handler outside both middlewares.

Core coordination commands stay in `Ruvio.Client`. No separate package repeats
them, and no server entry, ordinary command path, replication rule or `ONCE`
semantics changes for these integrations. The accompanying server fix preserves
key-level absolute deadlines when a Lua script is rolled back. It adds a deadline
to the existing script undo snapshot only, not to ordinary stored entries.

HTTP idempotency now uses the native `IDEM` state machine, not Lua. Its
canonical WAL opcode 53 stores the packed byte record and original deadline
atomically without widening ordinary entries. Upgrade servers, replicas and
recovery tools together. Earlier unpublished Lua hash records fail closed; see
the idempotency package README for the drain-before-upgrade requirement.

## Cost and correctness boundaries

The custom middleware is opt-in. Unselected endpoints bypass service resolution, network
calls, request/response buffering and feature telemetry. Selected endpoints pay
for their own keys, bounded buffers and commands. No package installs a background
polling loop, timer, exporter or per-application-tenant local dictionary.
Native Output Cache retains its own framework policy/serialization costs; its
Ruvio client is lazy until the first storage operation. Data Protection retains
its framework-owned in-process key-ring cache and refresh behavior.

Do not use eviction-prone or asynchronously replicated state as an unconditional
exactly-once guarantee. Idempotency retention is bounded; external side effects
require their own idempotency/transaction mechanism. A timeout after a write is an
unknown outcome, not permission to retry it automatically.

Each package documents its ActivitySource/Meter names where provided. Configure
OpenTelemetry/exporters in the host; metrics must not contain keys, payloads,
tenant identifiers or HTTP idempotency keys.

## Development and distribution

Targets .NET 8. Integration package versions start at `0.1.0`, independent of server
versions. Project references ensure package dependencies match the client version
built from this checkout. Publish the required client version before publishing an
integration; these changes are not automatically published.

```bash
dotnet test integrations/dotnet/Ruvio.Extensions.Caching.Tests
dotnet test integrations/dotnet/Ruvio.AspNetCore.RateLimiting.Tests
dotnet test integrations/dotnet/Ruvio.AspNetCore.Idempotency.Tests
dotnet test integrations/dotnet/Ruvio.AspNetCore.OutputCaching.Tests
dotnet test integrations/dotnet/Ruvio.AspNetCore.DataProtection.Tests
dotnet pack integrations/dotnet/Ruvio.Extensions.Caching -c Release -o dist
```

Set `RUVIO_TEST_ADDR=127.0.0.1:16382` to run live tests against an isolated Ruvio
server. `scripts/run_client_suites.py --languages ruvio-csharp --port 16382` includes
all integration suites; the existing manual CI clients job invokes this runner.
Unset live-test addresses run only the non-network checks. Never point tests at a
production server.

Use the manual `integration-packages` workflow to test and pack one package or
`all`. The artifact contains the `.nupkg` and `.snupkg`. Publishing is a
separate input and skips a version already on NuGet. Each package depends on
the `Ruvio.Client` version in this checkout, so publish that client first.
Update the selected integration's `Version` for a release; a server release
does not bump it.
