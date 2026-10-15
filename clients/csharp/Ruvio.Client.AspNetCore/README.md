# Ruvio.Client.AspNetCore

Registers [`Ruvio.Client`](https://www.nuget.org/packages/Ruvio.Client) in an ASP.NET Core service collection. The connection owner and database-scoped command client are separate singletons under one DI lifetime. The client speaks RESP2 and does not send `CLIENT SETINFO`.

```csharp
builder.Services.AddRuvioClient("127.0.0.1", 6379, database: 2);
```

With options:

```csharp
builder.Services.AddRuvioClient(options =>
{
    options.Host = "127.0.0.1";
    options.Port = 6379;
    options.Database = 2;
    options.Reconnect = true;
    options.MaxReconnectAttempts = 8;
    options.ReconnectBaseDelay = TimeSpan.FromMilliseconds(100);
    options.ReconnectMaxDelay = TimeSpan.FromSeconds(2);
}, connection =>
{
    connection.ConnectionLost += (_, notice) =>
        Console.WriteLine($"lost {notice.Host}:{notice.Port} subscriber={notice.Subscriber}");
    connection.ConnectionRestored += (_, notice) =>
        Console.WriteLine($"restored {notice.Host}:{notice.Port}");
});
```

Configuration section:

```csharp
builder.Services.AddRuvioClient(builder.Configuration.GetSection("Ruvio"));
```

```json
{
  "Ruvio": {
    "Host": "127.0.0.1",
    "Port": 6379,
    "Database": 2,
    "Reconnect": true,
    "MaxReconnectAttempts": 8,
    "ReconnectBaseDelay": "00:00:00.100",
    "ReconnectMaxDelay": "00:00:02"
  }
}
```

`Reconnect` defaults to true. A broken command or shard socket is opened again before the next command. The command that discovers the drop fails, so a write is not sent twice. A single `GET`, `MGET`, `EXISTS`, `HGET`, `TTL`, or `PTTL` is sent once more when no reply byte arrived. A pipeline is not retried. Pub/Sub channels are subscribed again on the new socket. Set `Reconnect` to false and call `IRuvioConnection.ReconnectAsync` when the application should own the timing. `ReconnectDelay` is a `Func<int, TimeSpan?>` and is not read from configuration; return null to stop. When it is set, `MaxReconnectAttempts` is not applied. The connection callback runs once, after the initial database client is open, so lifecycle handlers are attached before the singleton is handed out. `RuvioConnectionNotice` carries host, port, whether the socket is the Pub/Sub connection, and the error on a drop.

Connect sends `CLUSTER SLOTS` and, on a sharded server, routes by hash slot. Database 0 sends nothing after that; any other index sends `SELECT` on its own standalone command socket. Cluster mode has only database 0. The injected client is fixed to `RuvioClientOptions.Database`; use `IRuvioConnection.GetDatabaseAsync(index)` for another database, which returns a separately cached client.

Inject `IRuvioClient` into services that send commands. Inject `IRuvioConnection` where code creates database clients, reconnects, or handles lifecycle events. Disposing the DI container disposes the connection and every database client it created. Resolve `RuvioConnection` once at startup if the process should fail before it accepts traffic:

```csharp
app.Services.GetRequiredService<RuvioConnection>();
```

## Optional health check

```csharp
builder.Services.AddHealthChecks().AddRuvio(tags: ["ready"]);
app.MapHealthChecks("/health/ready");
```

The check sends a bounded `PING` through the existing `IRuvioClient`. It creates no
polling loop and runs only when the health-check service is invoked. The default
timeout is five seconds. It checks command-connection reachability, not every
shard, replica freshness, persistence health, or write availability. Prefer a
readiness endpoint rather than restarting the application when its dependency is
unavailable.

The separately packaged [.NET integrations](../../../integrations/dotnet/README.md)
provide `IDistributedCache`, ASP.NET Core output caching, Data Protection key-ring
storage, opt-in distributed rate limiting and HTTP idempotency.
Register this client's singleton once; integration packages neither own nor dispose it.
Data Protection can select a separate durable, non-evicting connection through
its connection-factory overload instead of sharing an eviction-prone cache deployment.
