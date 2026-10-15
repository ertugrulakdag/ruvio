using System.Collections.Concurrent;
using System.Net;

namespace Ruvio.Client;

/// <summary>
/// Owns the server connection and all database-scoped clients created from it.
/// Disposing this connection closes every child client and its sockets.
/// </summary>
public sealed class RuvioConnection : IRuvioConnection
{
    private readonly RuvioClientOptions _options;
    private readonly SemaphoreSlim _databaseGate = new(1, 1);
    private readonly ConcurrentDictionary<int, RuvioClient> _clients = new();
    private int _disposed;

    private RuvioConnection(RuvioClientOptions options, RuvioClient initialClient)
    {
        _options = options;
        AddClient(initialClient);
    }

    /// <summary>Host used to open the seed connection.</summary>
    public string Host => _options.Host;

    /// <summary>RESP port used to open the seed connection.</summary>
    public int Port => _options.Port;

    /// <summary>The database selected by the connection's options when it was opened.</summary>
    public int DefaultDatabase => _options.Database;

    /// <summary>True when every created database client has a live command connection.</summary>
    public bool IsConnected =>
        Volatile.Read(ref _disposed) == 0 && _clients.Values.All(client => client.IsConnected);

    /// <summary>True when the server exposed a multi-shard cluster topology.</summary>
    public bool IsCluster => _clients.Values.FirstOrDefault()?.IsCluster ?? false;

    /// <summary>Number of shards, or one for a standalone server.</summary>
    public int ShardCount => _clients.Values.FirstOrDefault()?.ShardCount ?? 0;

    /// <summary>Raised when a command or subscriber socket fails.</summary>
    public event EventHandler<Exception>? ConnectionFailed;

    /// <summary>Raised when a command or subscriber socket drops.</summary>
    public event EventHandler<RuvioConnectionNotice>? ConnectionLost;

    /// <summary>Raised after a dropped socket is restored.</summary>
    public event EventHandler<RuvioConnectionNotice>? ConnectionRestored;

    /// <summary>Opens the connection and its initial database client.</summary>
    public static Task<RuvioConnection> ConnectAsync(
        string host,
        int port,
        CancellationToken cancellationToken = default) =>
        ConnectAsync(new RuvioClientOptions { Host = host, Port = port }, cancellationToken);

    /// <summary>Opens the connection and its initial database client.</summary>
    public static Task<RuvioConnection> ConnectAsync(
        string host,
        int port,
        int database,
        CancellationToken cancellationToken = default) =>
        ConnectAsync(new RuvioClientOptions { Host = host, Port = port, Database = database }, cancellationToken);

    /// <summary>Opens the connection to an IP address.</summary>
    public static Task<RuvioConnection> ConnectAsync(
        IPAddress address,
        int port,
        int database = 0,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(address);
        return ConnectAsync(address.ToString(), port, database, cancellationToken);
    }

    /// <summary>Opens the connection to an endpoint.</summary>
    public static Task<RuvioConnection> ConnectAsync(
        IPEndPoint endPoint,
        int database = 0,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(endPoint);

        return ConnectAsync(endPoint.Address, endPoint.Port, database, cancellationToken);
    }

    /// <summary>Opens the connection with the supplied authentication, routing, and retry options.</summary>
    public static async Task<RuvioConnection> ConnectAsync(
        RuvioClientOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);

        RuvioClientOptions ownedOptions = CopyOptions(options, options.Database);
        RuvioClient initialClient = await RuvioClient.ConnectClientAsync(ownedOptions, cancellationToken)
            .ConfigureAwait(false);

        return new RuvioConnection(ownedOptions, initialClient);
    }

    /// <summary>
    /// Gets the one shared command client for a logical database. Standalone clients use
    /// separate command sockets so concurrent database clients cannot race through SELECT.
    /// Cluster mode supports database 0 only.
    /// </summary>
    public async Task<RuvioClient> GetDatabaseAsync(
        int index,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        ArgumentOutOfRangeException.ThrowIfNegative(index);

        await _databaseGate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

            if (_clients.TryGetValue(index, out RuvioClient? existing))
            {
                return existing;
            }

            if (IsCluster && index != 0)
            {
                throw new RuvioException("SELECT is not supported in cluster mode");
            }

            RuvioClientOptions databaseOptions = CopyOptions(_options, index);
            RuvioClient client = await RuvioClient.ConnectClientAsync(databaseOptions, cancellationToken)
                                                  .ConfigureAwait(false);
                
            AddClient(client);
            return client;
        }
        finally
        {
            _databaseGate.Release();
        }
    }

    /// <summary>Gets the database client selected by the connection options.</summary>
    public Task<RuvioClient> GetDefaultDatabaseAsync(CancellationToken cancellationToken = default) =>
        GetDatabaseAsync(DefaultDatabase, cancellationToken);

    async Task<IRuvioClient> IRuvioConnection.GetDefaultDatabaseAsync(CancellationToken cancellationToken) =>
        await GetDefaultDatabaseAsync(cancellationToken).ConfigureAwait(false);

    async Task<IRuvioClient> IRuvioConnection.GetDatabaseAsync(
        int index,
        CancellationToken cancellationToken) =>
        await GetDatabaseAsync(index, cancellationToken).ConfigureAwait(false);

    /// <summary>Reconnects every database client whose command connections are broken.</summary>
    public async Task ReconnectAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        
        await _databaseGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            await Task.WhenAll(_clients.Values.Select(client => client.ReconnectAsync(cancellationToken)))
                .ConfigureAwait(false);
        }
        finally
        {
            _databaseGate.Release();
        }
    }

    private void AddClient(RuvioClient client)
    {
        if (!_clients.TryAdd(client.Database, client))
        {
            client.Dispose();
            throw new InvalidOperationException($"A client for database {client.Database} already exists.");
        }

        client.ConnectionFailed += OnConnectionFailed;
        client.ConnectionLost += OnConnectionLost;
        client.ConnectionRestored += OnConnectionRestored;
    }

    private void OnConnectionFailed(object? sender, Exception exception) =>
        ConnectionFailed?.Invoke(this, exception);

    private void OnConnectionLost(object? sender, RuvioConnectionNotice notice) =>
        ConnectionLost?.Invoke(this, notice);

    private void OnConnectionRestored(object? sender, RuvioConnectionNotice notice) =>
        ConnectionRestored?.Invoke(this, notice);

    /// <summary>Closes the connection and disposes every database client it created.</summary>
    public void Dispose()
    {
        _databaseGate.Wait();
        
        try
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            foreach (RuvioClient client in _clients.Values)
            {
                client.ConnectionFailed -= OnConnectionFailed;
                client.ConnectionLost -= OnConnectionLost;
                client.ConnectionRestored -= OnConnectionRestored;
                client.Dispose();
            }
            _clients.Clear();
        }
        finally
        {
            _databaseGate.Release();
        }
    }

    /// <summary>Asynchronously disposes the connection and all its database clients.</summary>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    private static RuvioClientOptions CopyOptions(RuvioClientOptions options, int database) => new()
    {
        Host = options.Host,
        Port = options.Port,
        Username = options.Username,
        Password = options.Password,
        Database = database,
        ConnectTimeout = options.ConnectTimeout,
        MaxInFlightRequests = options.MaxInFlightRequests,
        DiscoverCluster = options.DiscoverCluster,
        Reconnect = options.Reconnect,
        MaxReconnectAttempts = options.MaxReconnectAttempts,
        ReconnectBaseDelay = options.ReconnectBaseDelay,
        ReconnectMaxDelay = options.ReconnectMaxDelay,
        ReconnectDelay = options.ReconnectDelay,
    };
}

/// <summary>
/// Extension methods for <see cref="IRuvioConnection"/>.
/// </summary>
public static class RuvioConnectionExtensions
{
    /// <summary>
    /// Gets the default database client from an asynchronous connection task.
    /// </summary>
    /// <param name="connectionTask">The task representing the asynchronous connection.</param>
    /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
    /// <returns>The default database client.</returns>
    public static async Task<IRuvioClient> GetDefaultDatabaseAsync(this Task<IRuvioConnection> connectionTask, CancellationToken cancellationToken = default)
    {
        var connection = await connectionTask.ConfigureAwait(false);
        return await connection.GetDefaultDatabaseAsync(cancellationToken).ConfigureAwait(false);
    }
}