namespace Ruvio.Client;

/// <summary>
/// Owns a server connection and the database-scoped command clients created from it.
/// Commands stay on <see cref="IRuvioClient"/>; disposing this owner disposes every client.
/// </summary>
public interface IRuvioConnection : IAsyncDisposable, IDisposable
{
    /// <summary>Host used to open the seed connection.</summary>
    string Host { get; }

    /// <summary>RESP port used to open the seed connection.</summary>
    int Port { get; }

    /// <summary>True when every created database client has a live command connection.</summary>
    bool IsConnected { get; }

    /// <summary>True when the server reported more than one shard.</summary>
    bool IsCluster { get; }

    /// <summary>How many shards the client routes to. 1 for a single server.</summary>
    int ShardCount { get; }

    /// <summary>The database selected by the connection's options when it was opened.</summary>
    int DefaultDatabase { get; }

    /// <summary>A command or shard socket failed and will not be retried, or the retry budget ran out.</summary>
    event EventHandler<Exception>? ConnectionFailed;

    /// <summary>A socket closed. The notice names the host, port, and whether it was the subscriber.</summary>
    event EventHandler<RuvioConnectionNotice>? ConnectionLost;

    /// <summary>A replacement socket is open.</summary>
    event EventHandler<RuvioConnectionNotice>? ConnectionRestored;

    /// <summary>Opens a new seed socket and, on a sharded server, rediscovers slots.</summary>
    Task ReconnectAsync(CancellationToken cancellationToken = default);

    /// <summary>Returns the shared command client bound to one logical database.</summary>
    Task<IRuvioClient> GetDatabaseAsync(int index, CancellationToken cancellationToken = default);

    /// <summary>Returns the shared command client for <see cref="DefaultDatabase"/>.</summary>
    Task<IRuvioClient> GetDefaultDatabaseAsync(CancellationToken cancellationToken = default);
}
