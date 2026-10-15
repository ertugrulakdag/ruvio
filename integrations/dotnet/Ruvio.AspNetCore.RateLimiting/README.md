# Ruvio.AspNetCore.RateLimiting

Opt-in distributed, first-request-anchored fixed-window HTTP budgets using
Ruvio's atomic `LIMIT`. This is middleware, not an implementation of the
in-process ASP.NET Core `RateLimiter`. There is no waiting queue or background task.

```csharp
using Ruvio.AspNetCore.RateLimiting;
using Ruvio.Client.AspNetCore;

builder.Services.AddRuvioClient(builder.Configuration.GetSection("Ruvio"));
builder.Services.AddRuvioRateLimiting(options =>
{
    options.KeyPrefix = "orders:rate:v1:";
    options.PartitionKey = context =>
        context.User.FindFirst("tenant_id")?.Value
        ?? throw new InvalidOperationException("An authenticated tenant is required.");
});

// After building the application:
app.UseRouting();
app.UseAuthentication();
app.UseAuthorization();
app.UseRuvioRateLimiting();
app.MapPost("/orders", HandleOrder)
    .RequireAuthorization()
    .WithMetadata(new RuvioRateLimitAttribute("create-order", 100, 60));
```

`AddRuvioClient` registers the connection owner and the shared database-scoped
`IRuvioClient`. This middleware does not own or dispose the connection. A missing
partition is a configuration error, not an anonymous shared bucket. Configure an explicit constant for global
limits. Partition values must be trusted and bounded, not arbitrary client headers.
The partition and policy are hashed; raw identifiers are not stored in keys or
telemetry. Every application should have its own prefix.

Only opted-in endpoints resolve services or call Ruvio. Other endpoints return
the next delegate's task directly: no async state machine, hashing, timers, network
requests, or per-request heap allocation in the bypass path. On an opted-in endpoint
there is one atomic command, no client-side counter and no extra deadline lookup.
There is no change to the server's ordinary GET/SET/INCR paths.

Use the same permit count and window for every endpoint/instance sharing a policy.
Changing settings during a live window does not reset its stored state. Rejected
requests return 429 and a conservative full-window `Retry-After`, not an exact reset
time. Backend failures propagate to the application's exception handler; configure
that handler to return an appropriate 503. The middleware never silently fails open
or retries a possibly-applied `LIMIT`.

Eviction, data loss, deletion and failover can reset a budget. Use suitable memory
and durability policies; this is not a billing/quota ledger. Fixed windows allow
bursts around window boundaries.

Optional OpenTelemetry subscriptions: ActivitySource and Meter
`Ruvio.AspNetCore.RateLimiting`. Counters are accepted, rejected and errors; no
user/key tags. No exporters or telemetry worker are installed by this package.

Build/package with `dotnet pack -c Release`. Targets .NET 8; requires the client
and server `LIMIT` command. The package is versioned separately from the server.
