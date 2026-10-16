namespace Ruvio.Client;

/// <summary>How <c>SET</c> treats a key that already exists or is missing.</summary>
public enum SetMode
{
    /// <summary>Write the key either way.</summary>
    Always = 0,

    /// <summary>Write only when the key is absent (<c>NX</c>).</summary>
    IfNotExists = 1,

    /// <summary>Write only when the key exists (<c>XX</c>).</summary>
    IfExists = 2,
}

/// <summary>Where to connect, and an optional password sent as <c>AUTH</c>.</summary>
public sealed class RuvioClientOptions
{
    /// <summary>Server host. Defaults to loopback.</summary>
    public string Host { get; set; } = "127.0.0.1";

    /// <summary>RESP port. Defaults to 6379.</summary>
    public int Port { get; set; } = 6379;

    /// <summary>ACL user. When set together with <see cref="Password"/>, the client sends <c>AUTH user password</c>.</summary>
    public string? Username { get; set; }

    /// <summary>When set, the client sends <c>AUTH</c> before <see cref="RuvioConnection.ConnectAsync(RuvioClientOptions, CancellationToken)"/> returns.</summary>
    public string? Password { get; set; }

    /// <summary>
    /// Logical database for a standalone server. Its database-scoped client sends
    /// <c>SELECT</c> after <c>AUTH</c>. Cluster mode supports database 0 only.
    /// </summary>
    public int Database { get; set; }

    /// <summary>
    /// Serializer settings the typed JSON methods (<c>JsonSetValueAsync</c>, <c>JsonGetValueAsync</c>, ...)
    /// use when a call passes none. Null means <see cref="System.Text.Json.JsonSerializerOptions.Default"/>.
    /// </summary>
    public System.Text.Json.JsonSerializerOptions? JsonSerializerOptions { get; set; }

    /// <summary>How long TCP connect may take.</summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Maximum admitted command groups per socket, including queued writes and pending replies.
    /// Each ExecuteManyAsync pipeline is one group. Defaults to 256; must be positive.
    /// </summary>
    public int MaxInFlightRequests { get; set; } = 256;

    /// <summary>
    /// When true (the default), connect sends <c>CLUSTER SLOTS</c>. A map with two or
    /// more shards turns the client into a router: later commands go to the shard that
    /// owns the key. A disabled-cluster error, or a single shard, keeps one socket.
    /// </summary>
    public bool DiscoverCluster { get; set; } = true;

    /// <summary>
    /// When true (the default), a broken command or shard socket is opened again before the
    /// next command. The command that discovers the drop fails and is never sent again, even
    /// a read whose reply had not started. Set this to false to reconnect only from <c>ReconnectAsync</c>.
    /// </summary>
    public bool Reconnect { get; set; } = true;

    /// <summary>How many times a reconnect tries <c>connect</c> before giving up. One means a single try.</summary>
    public int MaxReconnectAttempts { get; set; } = 8;

    /// <summary>First automatic wait after a failed reconnect attempt.</summary>
    public TimeSpan ReconnectBaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Ceiling for the automatic exponential wait.</summary>
    public TimeSpan ReconnectMaxDelay { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Replaces the built-in delay. The attempt argument starts at 1 after the first failure.
    /// Return null to stop. When this is set, <see cref="MaxReconnectAttempts"/> is not applied.
    /// </summary>
    public Func<int, TimeSpan?>? ReconnectDelay { get; set; }
}

/// <summary>A command or subscriber socket dropped, or came back.</summary>
public sealed class RuvioConnectionNotice : EventArgs
{
    internal RuvioConnectionNotice(string host, int port, bool subscriber, Exception? error)
    {
        Host = host;
        Port = port;
        Subscriber = subscriber;
        Error = error;
    }

    /// <summary>Host the socket was using.</summary>
    public string Host { get; }

    /// <summary>Port the socket was using.</summary>
    public int Port { get; }

    /// <summary>True when the socket is the dedicated Pub/Sub connection.</summary>
    public bool Subscriber { get; }

    /// <summary>Present for a drop. Null when the socket was restored.</summary>
    public Exception? Error { get; }
}
