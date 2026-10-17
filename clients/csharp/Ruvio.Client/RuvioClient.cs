using System.Runtime.CompilerServices;
using System.Globalization;
using System.Threading.Channels;

namespace Ruvio.Client;

/// <summary>
/// A command client bound to one logical database. Its owning
/// <see cref="RuvioConnection"/> manages its sockets and lifetime.
/// </summary>
public sealed partial class RuvioClient : IRuvioClient
{
    private readonly SemaphoreSlim _reconnectGate = new(1, 1);
    private RuvioSocket _seed;
    private readonly RuvioClientOptions _options;
    private readonly object _pubsubGate = new();
    private readonly Dictionary<string, PubSubLink> _pubsub = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, List<string[]>> _subscriptions = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, List<object>> _handlers = new(StringComparer.Ordinal);
    private readonly int _database;
    private ClusterRouter? _cluster;
    private bool _disposed;

    private RuvioClient(RuvioSocket seed, RuvioClientOptions options, ClusterRouter? cluster)
    {
        _seed = seed;
        _options = options;
        _database = options.Database;
        _cluster = cluster;

        cluster?.Bind(
            OpenWithRetryAsync,
            (host, port, error) => NoteSocketLost(host, port, error, subscriber: false),
            (host, port) => NoteSocketRestored(host, port, subscriber: false));
    }

    /// <summary>The logical database currently selected by this client.</summary>
    public int Database => _database;

    /// <summary>Host configured for this database client's command connection.</summary>
    public string Host => _options.Host;

    /// <summary>RESP port configured for this database client's command connection.</summary>
    public int Port => _options.Port;

    internal bool IsCluster => _cluster is not null;

    internal int ShardCount => _cluster?.NodeCount ?? 1;

    internal bool IsConnected => !_disposed && (_cluster?.HasLiveConnection() ?? !_seed.IsBroken);

    /// <summary>Raised for every <c>message</c>, <c>pmessage</c>, and <c>smessage</c> on a subscriber socket.</summary>
    public event EventHandler<PubSubMessage>? Message;

    /// <summary>Raised when a command or subscriber socket fails.</summary>
    internal event EventHandler<Exception>? ConnectionFailed;

    /// <summary>Raised when a command or subscriber socket drops. <see cref="ConnectionFailed"/> fires as well.</summary>
    internal event EventHandler<RuvioConnectionNotice>? ConnectionLost;

    /// <summary>Raised after a dropped socket is open again. Pub/Sub channels are subscribed again first.</summary>
    internal event EventHandler<RuvioConnectionNotice>? ConnectionRestored;

    /// <summary>
    /// Opens a dropped command socket again. With <see cref="RuvioClientOptions.Reconnect"/> on,
    /// the next command does this itself. Call it when that flag is off and the retry timing is yours.
    /// </summary>
    internal async Task ReconnectAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        if (_cluster is not null)
        {
            await _cluster.ReconnectBrokenAsync(cancellationToken);

            return;
        }

        if (!_seed.IsBroken)
        {
            return;
        }

        await ReconnectSeedAsync(cancellationToken);
    }

    /// <summary>The cluster hash slot (0-16383) of <paramref name="key"/>, honoring <c>{hash tags}</c>.</summary>
    public static int GetHashSlot(string key)
    {
        ArgumentNullException.ThrowIfNull(key);

        return ClusterKeys.Slot(key);
    }

    /// <summary>
    /// Opens a database-scoped client. When <see cref="RuvioClientOptions.Password"/> is set,
    /// sends <c>AUTH</c> before returning. When cluster discovery is enabled, discovers
    /// the shards; standalone servers use one socket.
    /// </summary>
    internal static async Task<RuvioClient> ConnectClientAsync(
        RuvioClientOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.Host))
        {
            throw new ArgumentException("Host is required.", nameof(options));
        }

        if (options.Port is < 1 or > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "Port must be between 1 and 65535.");
        }

        if (options.Database < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "Database must not be negative.");
        }

        RuvioSocket seed = await RuvioSocket.OpenAsync(
            options.Host,
            options.Port,
            options,
            cancellationToken);

        if (!options.DiscoverCluster)
        {
            try
            {
                await SelectIfNeededAsync(seed, options.Database, cancellationToken);

                return new RuvioClient(seed, options, cluster: null);
            }

            catch
            {
                seed.Dispose();
                throw;
            }
        }

        ClusterRouter? cluster = null;

        try
        {
            cluster = await ClusterRouter.DiscoverAsync(seed, options, cancellationToken);

            if (cluster is not null && options.Database != 0)
            {
                throw new RuvioException("SELECT is not supported in cluster mode");
            }

            if (cluster is null)
            {
                await SelectIfNeededAsync(seed, options.Database, cancellationToken);
            }

            return new RuvioClient(seed, options, cluster);
        }
        catch
        {
            if (cluster is not null)
            {
                cluster.Dispose();
            }
            else
            {
                seed.Dispose();
            }

            throw;
        }
    }

    private static async Task SelectIfNeededAsync(
        RuvioSocket connection,
        int database,
        CancellationToken cancellationToken)
    {
        if (database == 0)
        {
            return;
        }

        string index = database.ToString(CultureInfo.InvariantCulture);

        RuvioSocket.ThrowOnError(await connection.SendAsync(["SELECT", index], cancellationToken));
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> PingAsync(CancellationToken cancellationToken = default)
    {
        return ReadPing(await ExecuteCommandAsync(new RespCommand("PING"), cancellationToken));
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> PingAsync(string message, CancellationToken cancellationToken = default)
    {
        return ReadPing(await ExecuteCommandAsync(new RespCommand("PING", message), cancellationToken));
    }

    /// <summary>
    /// Pub/Sub mode answers <c>PING</c> with an array <c>pong</c> / payload, not
    /// <c>+PONG</c>.
    /// </summary>
    private static string ReadPing(RespValue reply)
    {
        if (reply.Items is { Count: >= 1 } && string.Equals(reply.Items[0].AsString(), "pong", StringComparison.OrdinalIgnoreCase))
        {
            return reply.Items.Count > 1
                ? reply.Items[1].AsString() ?? ""
                : "PONG";
        }

        return reply.AsString() ?? throw new RuvioException("PING returned a null reply");
    }

    /// <inheritdoc />
    public ValueTask<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        return GetBulkAsync(key, cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string?> GetStringAsync(string key, CancellationToken cancellationToken = default)
    {
        byte[]? value = await GetBulkAsync(key, cancellationToken);

        if (value is null)
        {
            return null;
        }

        return RespValue.Bulk(value).AsString();
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default)
    {
        byte[]? bytes = await GetBulkAsync(key, cancellationToken);
        Type type = typeof(T);
        bool requiredValue = type.IsValueType && Nullable.GetUnderlyingType(type) is null;

        if (bytes is null && requiredValue)
        {
            throw new RuvioException($"GET {key} is missing");
        }

        return RespConvert.FromBulk<T>(bytes);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<int?> GetInt32Async(string key, CancellationToken cancellationToken = default)
    {
        return RespConvert.FromBulk<int?>(await GetBulkAsync(key, cancellationToken));
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long?> GetInt64Async(string key, CancellationToken cancellationToken = default)
    {
        return RespConvert.FromBulk<long?>(await GetBulkAsync(key, cancellationToken));
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<byte[]?> GetBulkAsync(string key, CancellationToken cancellationToken)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("GET", key), cancellationToken);

        if (reply.Kind is RespKind.Null)
        {
            return null;
        }

        if (reply.Kind is not RespKind.Bulk)
        {
            throw new RuvioException($"GET returned {reply.Kind}");
        }

        return reply.Bytes;
    }

    /// <inheritdoc />
    public ValueTask<bool> SetAsync(
        string key,
        string value,
        CancellationToken cancellationToken = default)
    {
        return SetAsync(key, value, expiry: null, mode: SetMode.Always, cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> SetAsync(
        string key,
        string value,
        TimeSpan? expiry,
        SetMode mode = SetMode.Always,
        CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(SetCommand(key, value, expiry, mode), cancellationToken);

        if (reply.Kind is RespKind.Null)
        {
            return false;
        }

        return true;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> SetAsync(
        string key,
        string value,
        DateTimeOffset expiresAt,
        SetMode mode = SetMode.Always,
        CancellationToken cancellationToken = default)
    {
        RespCommand command = new RespCommand("SET", key, value).With("PXAT").With(expiresAt.ToUnixTimeMilliseconds());
        RespValue reply = await ExecuteCommandAsync(WithSetOptions(command, mode, get: false), cancellationToken);

        return reply.Kind is not RespKind.Null;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> SetKeepingTimeToLiveAsync(
        string key,
        string value,
        SetMode mode = SetMode.Always,
        CancellationToken cancellationToken = default)
    {
        RespCommand command = new RespCommand("SET", key, value).With("KEEPTTL");
        RespValue reply = await ExecuteCommandAsync(WithSetOptions(command, mode, get: false), cancellationToken);

        return reply.Kind is not RespKind.Null;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string?> SetAndGetAsync(
        string key,
        string value,
        TimeSpan? expiry = null,
        SetMode mode = SetMode.Always,
        CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(SetCommand(key, value, expiry, mode, get: true), cancellationToken);

        return reply.Kind is RespKind.Null ? null : reply.AsString();
    }

    private static RespCommand SetCommand(string key, string value, TimeSpan? expiry, SetMode mode, bool get = false)
    {
        var command = new RespCommand("SET", key, value);

        if (expiry is { } ttl)
        {
            if (ttl < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(expiry), "Expiry cannot be negative.");
            }

            command = command.With("PX").With((long)Math.Ceiling(ttl.TotalMilliseconds));
        }

        return WithSetOptions(command, mode, get);
    }

    private static RespCommand WithSetOptions(RespCommand command, SetMode mode, bool get)
    {
        if (mode == SetMode.IfNotExists)
        {
            command = command.With("NX");
        }
        else if (mode == SetMode.IfExists)
        {
            command = command.With("XX");
        }

        return get ? command.With("GET") : command;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long> IncrementAsync(string key, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("INCR", key), cancellationToken);

        if (reply.Kind is not RespKind.Integer)
        {
            throw new RuvioException($"INCR returned {reply.Kind}");
        }

        return reply.Integer;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> ExpireAsync(
        string key,
        TimeSpan ttl,
        CancellationToken cancellationToken = default)
    {
        if (ttl < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(ttl), "Expiry cannot be negative.");
        }

        long milliseconds = (long)Math.Ceiling(ttl.TotalMilliseconds);
        string[] arguments = milliseconds % 1000 == 0
            ? ["EXPIRE", key, (milliseconds / 1000).ToString(CultureInfo.InvariantCulture)]
            : ["PEXPIRE", key, milliseconds.ToString(CultureInfo.InvariantCulture)];

        RespValue reply = await ExecuteAsync(arguments, cancellationToken);

        if (reply.Kind is not RespKind.Integer)
        {
            throw new RuvioException($"EXPIRE returned {reply.Kind}");
        }

        return reply.Integer > 0;
    }

    /// <inheritdoc />
    public ValueTask<long> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        return DeleteAsync([key], cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long> DeleteAsync(
        IReadOnlyList<string> keys,
        CancellationToken cancellationToken = default)
    {
        if (keys.Count == 0)
        {
            throw new ArgumentException("DEL needs at least one key.", nameof(keys));
        }

        var arguments = new string[keys.Count + 1];

        arguments[0] = "DEL";

        for (int index = 0; index < keys.Count; index++)
        {
            arguments[index + 1] = keys[index];
        }

        RespValue reply = await ExecuteAsync(arguments, cancellationToken);

        if (reply.Kind is not RespKind.Integer)
        {
            throw new RuvioException($"DEL returned {reply.Kind}");
        }

        return reply.Integer;
    }

    /// <inheritdoc />
    public ValueTask<RespValue> ExecuteAsync(
        params string[] arguments)
    {
        return ExecuteAsync(arguments, CancellationToken.None);
    }

    /// <inheritdoc />
    public ValueTask<RespValue> ExecuteAsync(
        IReadOnlyList<string> arguments,
        CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand(arguments), cancellationToken);
    }

    internal ValueTask<RespValue> ExecuteCommandAsync(
        RespCommand command,
        CancellationToken cancellationToken)
    {
        // Hot path: one pooled request, one pooled await. Everything unusual takes the full path.
        if (_disposed || _cluster is not null || (_seed.IsBroken && _options.Reconnect) ||
            command.Count == 0 || IsSelect(command.Name))
        {
            return ExecuteSlowAsync(command, cancellationToken);
        }

        return AwaitReplyAsync(_seed.SendAsync(command, cancellationToken), command, cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<RespValue> AwaitReplyAsync(
        ValueTask<RespValue> pending,
        RespCommand command,
        CancellationToken cancellationToken)
    {
        RespValue reply = await pending.ConfigureAwait(false);

        return reply.Kind is RespKind.Error
            ? await RedirectOrThrowAsync(reply, command, cancellationToken).ConfigureAwait(false)
            : reply;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<RespValue> ExecuteSlowAsync(
        RespCommand command,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        if (command.Count == 0)
        {
            throw new ArgumentException("A command needs at least one argument.", nameof(command));
        }

        RejectSelect(command.Name);

        // A command whose socket dropped is not sent again; the next command reopens it.
        return await ExecuteOnceAsync(command, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<RespValue> ExecuteBinaryAsync(
        IReadOnlyList<ReadOnlyMemory<byte>> arguments,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(arguments);

        if (arguments.Count == 0)
        {
            throw new ArgumentException("A command needs at least one argument.", nameof(arguments));
        }

        ThrowIfDisposed();

        if (IsSelect(arguments[0].Span))
        {
            throw new RuvioException("SELECT is not allowed on a database-scoped client; request the database from its connection.");
        }

        cancellationToken.ThrowIfCancellationRequested();
        await EnsureCommandSocketAsync(cancellationToken).ConfigureAwait(false);

        if (_cluster is not null)
        {
            return RuvioSocket.ThrowOnError(
                await _cluster.ExecuteBinaryAsync(arguments, cancellationToken).ConfigureAwait(false));
        }

        RespValue reply = await _seed.SendBinaryAsync(arguments, cancellationToken).ConfigureAwait(false);

        if (ClusterRouter.MovedTarget(reply) is { } moved)
        {
            await AdoptClusterAsync(moved, cancellationToken).ConfigureAwait(false);

            if (_cluster is not null)
            {
                reply = await _cluster.ExecuteBinaryAsync(arguments, cancellationToken, redirects: 1).ConfigureAwait(false);
            }
        }

        return RuvioSocket.ThrowOnError(reply);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<RespValue> ExecuteOnceAsync(
        RespCommand command,
        CancellationToken cancellationToken)
    {
        await EnsureCommandSocketAsync(cancellationToken).ConfigureAwait(false);

        if (_cluster is not null)
        {
            return RuvioSocket.ThrowOnError(await _cluster.ExecuteAsync(command, cancellationToken).ConfigureAwait(false));
        }

        RespValue reply = await _seed.SendAsync(command, cancellationToken).ConfigureAwait(false);

        return reply.Kind is RespKind.Error
            ? await RedirectOrThrowAsync(reply, command, cancellationToken).ConfigureAwait(false)
            : reply;
    }

    /// <summary>Follows a <c>MOVED</c> by learning the slot map; any other error reply throws.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<RespValue> RedirectOrThrowAsync(
        RespValue reply,
        RespCommand command,
        CancellationToken cancellationToken)
    {
        if (ClusterRouter.MovedTarget(reply) is not { } moved)
        {
            return RuvioSocket.ThrowOnError(reply);
        }

        await AdoptClusterAsync(moved, cancellationToken).ConfigureAwait(false);

        if (_cluster is not null)
        {
            // The MOVED that brought the map in counts as the first redirect.
            reply = await _cluster.ExecuteAsync(command, cancellationToken, redirects: 1).ConfigureAwait(false);
        }

        return RuvioSocket.ThrowOnError(reply);
    }

    /// <summary>
    /// Learns the slot map after a <c>MOVED</c> (or a skipped discovery) and points the moved
    /// slot at the node the reply named. Later commands go to the shard that owns the key.
    /// </summary>
    private async Task AdoptClusterAsync((int Slot, string Host, int Port) moved, CancellationToken cancellationToken)
    {
        ClusterRouter? cluster = await ClusterRouter.DiscoverAsync(_seed, _options, cancellationToken);

        if (cluster is null)
        {
            return;
        }

        cluster.PointSlot(moved);
        _cluster = cluster;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<RespValue>> ExecuteManyAsync(
        IReadOnlyList<IReadOnlyList<string>> commands,
        CancellationToken cancellationToken = default)
    {
        if (commands.Count == 0)
        {
            throw new ArgumentException("A pipeline needs at least one command.", nameof(commands));
        }

        ThrowIfDisposed();

        for (int index = 0; index < commands.Count; index++)
        {
            RejectSelect(commands[index]);
        }

        await EnsureCommandSocketAsync(cancellationToken);

        return _cluster is null
            ? await _seed.SendManyAsync(commands, cancellationToken)
            : await _cluster.ExecuteManyAsync(commands, cancellationToken);
    }

    internal async Task<IReadOnlyList<RespValue>> ExecuteOnConnectionAsync(
        IReadOnlyList<string> arguments,
        int replies,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        PubSubLink link = await SubscriberAsync(ClusterKeys.Name(arguments), arguments, cancellationToken);

        return await link.SendAsync(arguments, replies, cancellationToken);
    }

    internal async Task<RespValue> ReadFromSubscriptionAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        PubSubLink link = FirstSubscriber()
            ?? await SubscriberAsync("SUBSCRIBE", ["SUBSCRIBE"], cancellationToken);

        return ToResp(await link.Messages.ReadAsync(cancellationToken));
    }

    private static RespValue ToResp(PubSubMessage message)
    {
        byte[] kind = System.Text.Encoding.UTF8.GetBytes(message.Kind);
        byte[] channel = System.Text.Encoding.UTF8.GetBytes(message.Channel);
        byte[] payload = System.Text.Encoding.UTF8.GetBytes(message.Payload);

        if (message.Kind == "pmessage")
        {
            return RespValue.Array(
            [
                RespValue.Bulk(kind),
                RespValue.Bulk(System.Text.Encoding.UTF8.GetBytes(message.Pattern ?? "")),
                RespValue.Bulk(channel),
                RespValue.Bulk(payload),
            ]);
        }

        return RespValue.Array(
        [
            RespValue.Bulk(kind),
            RespValue.Bulk(channel),
            RespValue.Bulk(payload),
        ]);
    }

    internal async Task<PubSubMessage> ReadPubSubDeliveryAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        while (true)
        {
            PubSubLink link = FirstSubscriber()
                ?? await SubscriberAsync("SUBSCRIBE", ["SUBSCRIBE"], cancellationToken);

            try
            {
                return await link.Messages.ReadAsync(cancellationToken);
            }

            catch (ChannelClosedException) when (_options.Reconnect && !_disposed)
            {
                await Task.Delay(20, cancellationToken);
            }
        }
    }

    private void TrackSubscription(IReadOnlyList<string> arguments)
    {
        if (arguments.Count == 0)
        {
            return;
        }

        string command = arguments[0].ToUpperInvariant();

        if (command is not ("SUBSCRIBE" or "UNSUBSCRIBE" or "PSUBSCRIBE" or "PUNSUBSCRIBE" or "SSUBSCRIBE" or "SUNSUBSCRIBE"))
        {
            return;
        }

        (string host, int port) = _cluster is null
            ? (_options.Host, _options.Port)
            : _cluster.SubscriptionEndpoint(command, arguments);

        string key = $"{host}:{port}";

        lock (_pubsubGate)
        {
            if (!_subscriptions.TryGetValue(key, out List<string[]>? list))
            {
                list = [];
                _subscriptions[key] = list;
            }

            if (command is "UNSUBSCRIBE" or "PUNSUBSCRIBE" or "SUNSUBSCRIBE")
            {
                string subscribe = command switch
                {
                    "UNSUBSCRIBE" => "SUBSCRIBE",
                    "PUNSUBSCRIBE" => "PSUBSCRIBE",
                    _ => "SSUBSCRIBE",
                };

                if (arguments.Count <= 1)
                {
                    list.RemoveAll(item => item[0].Equals(subscribe, StringComparison.OrdinalIgnoreCase));
                }
                else
                {
                    var drop = new HashSet<string>(arguments.Skip(1), StringComparer.Ordinal);

                    list.RemoveAll(item =>
                        item.Length > 1 &&
                        item[0].Equals(subscribe, StringComparison.OrdinalIgnoreCase) &&
                        drop.Contains(item[1]));
                }

                return;
            }

            for (int index = 1; index < arguments.Count; index++)
            {
                string channel = arguments[index];

                if (list.Any(item => item.Length == 2 && item[0].Equals(command, StringComparison.OrdinalIgnoreCase) && item[1] == channel))
                {
                    continue;
                }

                list.Add([command, channel]);
            }
        }
    }

    internal void RegisterHandler(string name, Action<PubSubMessage> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        AddHandler(name, handler);
    }

    internal void RegisterHandler(string name, Func<PubSubMessage, Task> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        AddHandler(name, handler);
    }

    internal void RemoveHandlers(params string[] names)
    {
        lock (_pubsubGate)
        {
            if (names.Length == 0)
            {
                _handlers.Clear();

                return;
            }

            foreach (string name in names)
            {
                _handlers.Remove(name);
            }
        }
    }

    private void AddHandler(string name, object handler)
    {
        lock (_pubsubGate)
        {
            if (!_handlers.TryGetValue(name, out List<object>? list))
            {
                list = [];
                _handlers[name] = list;
            }

            list.Add(handler);
        }
    }

    private async Task<PubSubLink> SubscriberAsync(
        string name,
        IReadOnlyList<string> arguments,
        CancellationToken cancellationToken)
    {
        (string host, int port) = _cluster is null
            ? (_options.Host, _options.Port)
            : _cluster.SubscriptionEndpoint(name, arguments);
        string key = $"{host}:{port}";

        lock (_pubsubGate)
        {
            if (_pubsub.TryGetValue(key, out PubSubLink? existing) && !existing.Connection.IsBroken)
            {
                return existing;
            }
        }

        RuvioSocket connection = await RuvioSocket.OpenAsync(host, port, _options, cancellationToken, subscriber: true);
        var link = new PubSubLink(connection, DispatchMessage, FailSubscriber);

        lock (_pubsubGate)
        {
            if (_pubsub.TryGetValue(key, out PubSubLink? raced) && !raced.Connection.IsBroken)
            {
                link.Dispose();

                return raced;
            }

            _pubsub[key] = link;
        }

        return link;
    }

    private PubSubLink? FirstSubscriber()
    {
        lock (_pubsubGate)
        {
            return _pubsub.Values.FirstOrDefault(link => !link.Connection.IsBroken);
        }
    }

    private void DispatchMessage(PubSubMessage message)
    {
        Message?.Invoke(this, message);
        object[] handlers;

        lock (_pubsubGate)
        {
            List<object> matched = [];

            if (_handlers.TryGetValue(message.Channel, out List<object>? byChannel))
            {
                matched.AddRange(byChannel);
            }

            if (message.Pattern is not null &&
                _handlers.TryGetValue(message.Pattern, out List<object>? byPattern))
            {
                matched.AddRange(byPattern);
            }

            handlers = [.. matched];
        }

        foreach (object handler in handlers)
        {
            switch (handler)
            {
                case Action<PubSubMessage> action:
                    action(message);
                    break;
                case Func<PubSubMessage, Task> asyncAction:
                    _ = asyncAction(message);
                    break;
            }
        }
    }

    private void FailSubscriber(Exception exception)
    {
        NoteSocketLost(_options.Host, _options.Port, exception, subscriber: true);

        if (!_options.Reconnect || _disposed)
        {
            return;
        }

        _ = RecoverSubscribersAsync();
    }

    private async Task EnsureCommandSocketAsync(CancellationToken cancellationToken)
    {
        if (_cluster is not null || !_seed.IsBroken || !_options.Reconnect)
        {
            return;
        }

        await ReconnectSeedAsync(cancellationToken);
    }

    private async Task ReconnectSeedAsync(CancellationToken cancellationToken)
    {
        await _reconnectGate.WaitAsync(cancellationToken);

        try
        {
            ThrowIfDisposed();

            if (!_seed.IsBroken)
            {
                return;
            }

            NoteSocketLost(_seed.Host, _seed.Port, new RuvioException("connection is closed"), subscriber: false);
            RuvioSocket opened = await OpenWithRetryAsync(_options.Host, _options.Port, cancellationToken);

            try
            {
                await SelectIfNeededAsync(opened, _options.Database, cancellationToken);
                ThrowIfDisposed();
            }

            catch
            {
                opened.Dispose();
                throw;
            }

            RuvioSocket previous = _seed;

            _seed = opened;
            previous.Dispose();
            NoteSocketRestored(opened.Host, opened.Port, subscriber: false);
        }

        finally
        {
            _reconnectGate.Release();
        }
    }

    private Task<RuvioSocket> OpenWithRetryAsync(string host, int port, CancellationToken cancellationToken) =>
        OpenWithRetryAsync(host, port, cancellationToken, subscriber: false);

    private async Task<RuvioSocket> OpenWithRetryAsync(string host, int port, CancellationToken cancellationToken, bool subscriber)
    {
        int attempt = 0;

        while (true)
        {
            attempt++;

            try
            {
                return await RuvioSocket.OpenAsync(host, port, _options, cancellationToken, subscriber);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                TimeSpan? delay = NextReconnectDelay(attempt);

                if (delay is null)
                {
                    throw;
                }

                await Task.Delay(delay.Value, cancellationToken);
            }
        }
    }

    private TimeSpan? NextReconnectDelay(int attempt)
    {
        if (!_options.Reconnect)
        {
            return null;
        }

        if (_options.ReconnectDelay is not null)
        {
            return _options.ReconnectDelay(attempt);
        }

        if (attempt >= _options.MaxReconnectAttempts)
        {
            return null;
        }

        double multiplier = Math.Pow(2, attempt - 1);
        var delay = TimeSpan.FromMilliseconds(_options.ReconnectBaseDelay.TotalMilliseconds * multiplier);

        return delay <= _options.ReconnectMaxDelay ? delay : _options.ReconnectMaxDelay;
    }

    private void NoteSocketLost(string host, int port, Exception error, bool subscriber)
    {
        ConnectionFailed?.Invoke(this, error);
        ConnectionLost?.Invoke(this, new RuvioConnectionNotice(host, port, subscriber, error));
    }

    private void NoteSocketRestored(string host, int port, bool subscriber)
    {
        ConnectionRestored?.Invoke(this, new RuvioConnectionNotice(host, port, subscriber, error: null));
    }

    private async Task RecoverSubscribersAsync()
    {
        string[] keys;

        lock (_pubsubGate)
        {
            keys = [.. _pubsub.Where(pair => pair.Value.Connection.IsBroken).Select(pair => pair.Key)];
        }

        foreach (string key in keys)
        {
            int split = key.LastIndexOf(':');

            if (split <= 0 || !int.TryParse(key[(split + 1)..], out int port))
            {
                continue;
            }

            string host = key[..split];

            try
            {
                await ReplaceSubscriberAsync(host, port, key);
                NoteSocketRestored(host, port, subscriber: true);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                NoteSocketLost(host, port, exception, subscriber: true);
            }
        }
    }

    private async Task ReplaceSubscriberAsync(string host, int port, string key)
    {
        RuvioSocket connection = await OpenWithRetryAsync(host, port, CancellationToken.None, subscriber: true);
        var link = new PubSubLink(connection, DispatchMessage, FailSubscriber);

        string[][] replay;

        lock (_pubsubGate)
        {
            replay = _subscriptions.TryGetValue(key, out List<string[]>? stored) ? [.. stored] : [];

            if (_pubsub.TryGetValue(key, out PubSubLink? previous))
            {
                previous.Dispose();
            }

            _pubsub[key] = link;
        }

        foreach (string[] command in replay)
        {
            await link.SendAsync(command, Math.Max(1, command.Length - 1), CancellationToken.None);
        }
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    private static void RejectSelect(IReadOnlyList<string> arguments) =>
        RejectSelect(arguments.Count > 0 ? arguments[0] : null);

    private static bool IsSelect(string? name) =>
        string.Equals(name, "SELECT", StringComparison.OrdinalIgnoreCase);

    private static void RejectSelect(string? name)
    {
        if (IsSelect(name))
        {
            throw new RuvioException("SELECT is not allowed on a database-scoped client; request the database from its connection.");
        }
    }

    private static bool IsSelect(ReadOnlySpan<byte> command) =>
        command.Length == 6
        && (command[0] is (byte)'S' or (byte)'s')
        && (command[1] is (byte)'E' or (byte)'e')
        && (command[2] is (byte)'L' or (byte)'l')
        && (command[3] is (byte)'E' or (byte)'e')
        && (command[4] is (byte)'C' or (byte)'c')
        && (command[5] is (byte)'T' or (byte)'t');

    /// <summary>Closes every socket this client opened, including per-shard connections.</summary>
    internal void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        lock (_pubsubGate)
        {
            foreach (PubSubLink link in _pubsub.Values)
            {
                link.Dispose();
            }

            _pubsub.Clear();
        }

        if (_cluster is not null)
        {
            _cluster.Dispose();

            return;
        }

        _seed.Dispose();
    }

    /// <summary>Closes the connection.</summary>
    internal ValueTask DisposeAsync()
    {
        Dispose();

        return ValueTask.CompletedTask;
    }
}
