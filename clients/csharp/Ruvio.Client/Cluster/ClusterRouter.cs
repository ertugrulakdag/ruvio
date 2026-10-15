using System.Runtime.CompilerServices;
using System.Globalization;
using System.Text;

namespace Ruvio.Client;

/// <summary>
/// Routes commands to the shard that owns their slot. Built from <c>CLUSTER SLOTS</c>;
/// a <c>MOVED</c> reply refreshes the map and retries.
/// </summary>
internal sealed class ClusterRouter : IDisposable
{
    private const int MaxRedirects = 3;

    private static readonly HashSet<string> Split = new(StringComparer.Ordinal)
    {
        "DEL", "EXISTS", "MGET", "MSET", "TOUCH", "UNLINK",
    };

    private static readonly HashSet<string> TransactionCommands = new(StringComparer.Ordinal)
    {
        "DISCARD", "EXEC", "MULTI", "UNWATCH", "WATCH",
    };

    private readonly RuvioClientOptions _options;
    private readonly RuvioSocket _seed;
    private readonly Dictionary<string, RuvioSocket> _connections = new(StringComparer.OrdinalIgnoreCase);
    private readonly SemaphoreSlim _connectGate = new(1, 1);
    private readonly SemaphoreSlim _refreshGate = new(1, 1);
    private readonly List<IReadOnlyList<string>> _queued = [];
    private ClusterTopology _topology;
    private RuvioSocket? _pinned;
    private bool _multiPending;
    private bool _inMulti;
    private bool _disposed;
    private Func<string, int, CancellationToken, Task<RuvioSocket>>? _open;
    private Action<string, int, Exception>? _lost;
    private Action<string, int>? _restored;

    private ClusterRouter(RuvioSocket seed, RuvioClientOptions options, ClusterTopology topology)
    {
        _seed = seed;
        _options = options;
        _topology = topology;
        _connections[Endpoint(seed.Host, seed.Port)] = seed;
    }

    public int NodeCount => _topology.Nodes.Count;

    internal bool HasLiveConnection()
    {
        lock (_connections)
        {
            return _connections.Values.Any(connection => !connection.IsBroken);
        }
    }

    internal void Bind(
        Func<string, int, CancellationToken, Task<RuvioSocket>> open,
        Action<string, int, Exception> lost,
        Action<string, int> restored)
    {
        _open = open;
        _lost = lost;
        _restored = restored;
    }

    internal async Task ReconnectBrokenAsync(CancellationToken cancellationToken)
    {
        (string Host, int Port)[] nodes = [.. _topology.Nodes];

        foreach ((string host, int port) in nodes)
        {
            await ConnectionAsync(host, port, cancellationToken, force: true);
        }
    }

    /// <summary>
    /// Asks the seed for <c>CLUSTER SLOTS</c>. Null when the server is not a cluster
    /// or has one node, so the caller keeps the plain single-socket path.
    /// </summary>
    public static async Task<ClusterRouter?> DiscoverAsync(
        RuvioSocket seed,
        RuvioClientOptions options,
        CancellationToken cancellationToken)
    {
        RespValue reply = await seed.SendAsync(["CLUSTER", "SLOTS"], cancellationToken);
        ClusterTopology? topology = ClusterTopology.Parse(reply);

        if (topology is null)
        {
            return null;
        }

        return new ClusterRouter(seed, options, topology);
    }

    public async Task<RespValue> ExecuteAsync(IReadOnlyList<string> arguments, CancellationToken cancellationToken)
    {
        string name = ClusterKeys.Name(arguments);

        if (_pinned is not null || _multiPending)
        {
            return await TransactionAsync(name, arguments, cancellationToken);
        }

        switch (name)
        {
            case "MULTI":
                _multiPending = true;

                return RespValue.Simple("OK");
            case "WATCH":
                return await WatchAsync(arguments, cancellationToken);
            case "DBSIZE":
            case "FLUSHDB":
            case "FLUSHALL":
                return await EveryNodeAsync(name, arguments, cancellationToken);
            case "SCRIPT" or "FUNCTION" when SpreadsToEveryNode(arguments):
                return await EveryNodeAsync(name, arguments, cancellationToken);
            case "SCAN":
                return await ScanAsync(arguments, cancellationToken);
            case "PUBLISH":
                return await SendRoutedAsync(await ForSlotAsync(0, cancellationToken), arguments, cancellationToken);
        }

        IReadOnlyList<string> keys = ClusterKeys.Of(name, arguments);

        if (name is "RENAME" or "RENAMENX" && keys.Count == 2 && SpansSlots(keys))
        {
            return await RenameAcrossAsync(name == "RENAMENX", keys[0], keys[1], cancellationToken);
        }

        if (Split.Contains(name) && SpansSlots(keys))
        {
            return await SplitAsync(name, arguments, keys, cancellationToken);
        }

        RuvioSocket connection = await TargetAsync(keys, cancellationToken);

        return await SendRoutedAsync(connection, arguments, cancellationToken);
    }

    /// <summary>
    /// Routes an ordinary single-key command without building a string list. Everything
    /// else (transactions, multi-key, fan-out) takes the list path.
    /// </summary>
    internal ValueTask<RespValue> ExecuteAsync(in RespCommand command, CancellationToken cancellationToken)
    {
        if (_pinned is null && !_multiPending && command.Count >= 2
            && command.Name is { } raw && command.TextAt(1) is { } key)
        {
            string name = raw.ToUpperInvariant();

            if (ClusterKeys.IsSingleKey(name))
            {
                return SendToSlotAsync(command, ClusterKeys.Slot(key), cancellationToken);
            }
        }

        return new ValueTask<RespValue>(ExecuteAsync(command.AsList(), cancellationToken));
    }

    // Pooled builder: each routed call reuses a state machine box instead of allocating a new one.
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<RespValue> SendToSlotAsync(RespCommand command, int slot, CancellationToken cancellationToken)
    {
        RuvioSocket connection = await ForSlotAsync(slot, cancellationToken).ConfigureAwait(false);

        for (int redirect = 0; ; redirect++)
        {
            RespValue reply = await connection.SendAsync(command, cancellationToken).ConfigureAwait(false);

            if (redirect >= MaxRedirects || MovedTarget(reply) is not { } node)
            {
                return reply;
            }

            await RefreshAsync(cancellationToken).ConfigureAwait(false);
            connection = await ConnectionAsync(node.Host, node.Port, cancellationToken).ConfigureAwait(false);
        }
    }

    internal async Task<RespValue> ExecuteBinaryAsync(
        IReadOnlyList<ReadOnlyMemory<byte>> arguments, CancellationToken cancellationToken)
    {
        if (_pinned is not null || _multiPending)
        {
            throw new NotSupportedException("Binary commands cannot be mixed with cluster transactions.");
        }

        string name = Encoding.ASCII.GetString(arguments[0].Span).ToUpperInvariant();
        int first = 1;
        int count;

        switch (name)
        {
            case "EVAL":
            case "EVALSHA":
                if (arguments.Count < 3 ||
                    !int.TryParse(Encoding.ASCII.GetString(arguments[2].Span), NumberStyles.None,
                        CultureInfo.InvariantCulture, out count) ||
                    count < 0 || count > arguments.Count - 3)
                {
                    throw new ArgumentException("Invalid script key count.", nameof(arguments));
                }

                first = 3;
                break;
            case "DEL": case "UNLINK": case "EXISTS":
                count = arguments.Count - 1;
                break;
            case "GET": case "SET": case "HGET": case "HSET": case "HMGET": case "HMSET":
            case "HDEL": case "PEXPIRE": case "PEXPIREAT": case "PTTL": case "TTL": case "PERSIST":
            case "IDEM":
                count = Math.Min(1, arguments.Count - 1);
                break;
            case "PING": case "ECHO":
                count = 0;
                break;
            default:
                throw new NotSupportedException($"Binary cluster routing is not supported for {name}.");
        }

        int slot = count == 0 ? 0 : ClusterKeys.Slot(arguments[first].Span);

        for (int index = 1; index < count; index++)
        {
            if (ClusterKeys.Slot(arguments[first + index].Span) != slot)
            {
                throw new RuvioException("CROSSSLOT binary commands require keys in one slot");
            }
        }

        RuvioSocket connection = await ForSlotAsync(slot, cancellationToken).ConfigureAwait(false);

        for (int redirect = 0; ; redirect++)
        {
            RespValue reply = await connection.SendBinaryAsync(arguments, cancellationToken).ConfigureAwait(false);

            if (redirect >= MaxRedirects || MovedTarget(reply) is not { } node)
            {
                return reply;
            }

            await RefreshAsync(cancellationToken).ConfigureAwait(false);
            connection = await ConnectionAsync(node.Host, node.Port, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Groups a pipeline by shard and sends each group as one write. Replies come back in
    /// command order. A pipeline that holds a transaction goes to one shard unchanged.
    /// </summary>
    public async Task<IReadOnlyList<RespValue>> ExecuteManyAsync(
        IReadOnlyList<IReadOnlyList<string>> commands,
        CancellationToken cancellationToken)
    {
        if (_pinned is not null || _multiPending || HasTransactionCommand(commands))
        {
            RuvioSocket single = _pinned ?? await FirstKeyedTargetAsync(commands, cancellationToken);

            return await single.SendManyAsync(commands, cancellationToken);
        }

        var groups = new Dictionary<RuvioSocket, List<int>>();

        for (int index = 0; index < commands.Count; index++)
        {
            IReadOnlyList<string> command = commands[index];
            RuvioSocket connection = await TargetKeyAsync(
                ClusterKeys.First(ClusterKeys.Name(command), command),
                cancellationToken);

            if (!groups.TryGetValue(connection, out List<int>? positions))
            {
                positions = [];
                groups[connection] = positions;
            }

            positions.Add(index);
        }

        var replies = new RespValue[commands.Count];

        if (groups.Count == 1)
        {
            foreach ((RuvioSocket only, List<int> _) in groups)
            {
                IReadOnlyList<RespValue> values = await only.SendManyAsync(commands, cancellationToken);

                for (int index = 0; index < values.Count; index++)
                {
                    replies[index] = values[index];
                }
            }
        }
        else
        {
            var sends = new Task[groups.Count];
            int next = 0;

            foreach ((RuvioSocket connection, List<int> positions) in groups)
            {
                var batch = new IReadOnlyList<string>[positions.Count];

                for (int index = 0; index < batch.Length; index++)
                {
                    batch[index] = commands[positions[index]];
                }

                sends[next++] = SendGroupAsync(connection, batch, positions, replies, cancellationToken);
            }

            await Task.WhenAll(sends);
        }

        for (int index = 0; index < replies.Length; index++)
        {
            if (MovedTarget(replies[index]) is { } node)
            {
                await RefreshAsync(cancellationToken);
                RuvioSocket owner = await ConnectionAsync(node.Host, node.Port, cancellationToken);

                replies[index] = await SendRoutedAsync(owner, commands[index], cancellationToken);
            }
        }

        return replies;
    }

    /// <summary>
    /// Picks the socket for a subscribe command. Sharded channels go to their slot owner;
    /// classic channels and patterns go to the owner of slot 0 so every client meets there.
    /// </summary>
    public Task<RuvioSocket> SubscriptionTargetAsync(
        string name,
        IReadOnlyList<string> arguments,
        CancellationToken cancellationToken)
    {
        (string host, int port) = SubscriptionEndpoint(name, arguments);

        return ConnectionAsync(host, port, cancellationToken);
    }

    /// <summary>
    /// Host and port for a dedicated subscriber socket. Classic channels meet on
    /// slot 0; sharded channels go to the slot owner. This is not a command socket.
    /// </summary>
    public (string Host, int Port) SubscriptionEndpoint(string name, IReadOnlyList<string> arguments)
    {
        int slot = name is "SSUBSCRIBE" or "SUNSUBSCRIBE" && arguments.Count > 1
            ? ClusterKeys.Slot(arguments[1])
            : 0;

        return _topology.Owner(slot) ?? (_seed.Host, _seed.Port);
    }

    private async Task<RespValue> TransactionAsync(
        string name,
        IReadOnlyList<string> arguments,
        CancellationToken cancellationToken)
    {
        if (_pinned is { } pinned)
        {
            RespValue reply = await pinned.SendAsync(arguments, cancellationToken);

            if (name is "EXEC" or "DISCARD" || (name == "UNWATCH" && !_inMulti))
            {
                _pinned = null;
                _inMulti = false;
            }
            else if (name == "MULTI" && reply.Kind is not RespKind.Error)
            {
                _inMulti = true;
            }

            return reply;
        }

        switch (name)
        {
            case "MULTI":
                return RespValue.Error("ERR MULTI calls can not be nested");
            case "WATCH":
                return RespValue.Error("ERR WATCH inside MULTI is not allowed");
            case "DISCARD":
                ResetTransaction();

                return RespValue.Simple("OK");
            case "EXEC":
                IReadOnlyList<RespValue> flushed = await StartTransactionAsync(
                    await TargetAsync([], cancellationToken),
                    [.. _queued, arguments],
                    cancellationToken);

                ResetTransaction();

                return flushed[^1];
        }

        IReadOnlyList<string> keys = ClusterKeys.Of(name, arguments);

        if (keys.Count == 0)
        {
            _queued.Add([.. arguments]);

            return RespValue.Simple("QUEUED");
        }

        RuvioSocket owner = await ForSlotAsync(ClusterKeys.Slot(keys[0]), cancellationToken);
        IReadOnlyList<RespValue> replies = await StartTransactionAsync(owner, [.. _queued, arguments], cancellationToken);

        _queued.Clear();
        _multiPending = false;

        if (replies[0].Kind is RespKind.Error)
        {
            return replies[0];
        }

        _pinned = owner;
        _inMulti = true;

        return replies[^1];
    }

    private static Task<IReadOnlyList<RespValue>> StartTransactionAsync(
        RuvioSocket connection,
        IReadOnlyList<IReadOnlyList<string>> commands,
        CancellationToken cancellationToken)
    {
        return connection.SendManyAsync([["MULTI"], .. commands], cancellationToken);
    }

    private void ResetTransaction()
    {
        _queued.Clear();
        _multiPending = false;
        _inMulti = false;
        _pinned = null;
    }

    private async Task<RespValue> WatchAsync(IReadOnlyList<string> arguments, CancellationToken cancellationToken)
    {
        RuvioSocket owner = await TargetAsync(ClusterKeys.Of("WATCH", arguments), cancellationToken);
        RespValue reply = await owner.SendAsync(arguments, cancellationToken);

        if (reply.Kind is not RespKind.Error)
        {
            _pinned = owner;
        }

        return reply;
    }

    private static bool SpreadsToEveryNode(IReadOnlyList<string> arguments)
    {
        if (arguments.Count < 2)
        {
            return false;
        }

        string subcommand = arguments[1].ToUpperInvariant();

        return subcommand is "LOAD" or "FLUSH" or "DELETE";
    }

    private async Task<RespValue> EveryNodeAsync(
        string name,
        IReadOnlyList<string> arguments,
        CancellationToken cancellationToken)
    {
        RuvioSocket[] connections = await EveryConnectionAsync(cancellationToken);
        var sends = new Task<RespValue>[connections.Length];

        for (int index = 0; index < connections.Length; index++)
        {
            sends[index] = connections[index].SendAsync(arguments, cancellationToken).AsTask();
        }

        RespValue[] replies = await Task.WhenAll(sends);
        long total = 0;

        foreach (RespValue reply in replies)
        {
            if (reply.Kind is RespKind.Error)
            {
                return reply;
            }

            total += reply.Integer;
        }

        return name == "DBSIZE" ? RespValue.FromInteger(total) : replies[0];
    }

    /// <summary>
    /// Walks each shard in turn. The client cursor is <c>serverCursor * nodes + node</c>,
    /// so 0 starts on the first shard and 0 comes back only after the last one.
    /// </summary>
    private async Task<RespValue> ScanAsync(IReadOnlyList<string> arguments, CancellationToken cancellationToken)
    {
        ulong nodes = (ulong)_topology.Nodes.Count;

        if (arguments.Count < 2
            || !ulong.TryParse(arguments[1], NumberStyles.None, CultureInfo.InvariantCulture, out ulong cursor))
        {
            return RespValue.Error("ERR invalid cursor");
        }

        int node = (int)(cursor % nodes);
        string[] forwarded = [.. arguments];

        forwarded[1] = (cursor / nodes).ToString(CultureInfo.InvariantCulture);
        (string host, int port) = _topology.Nodes[node];

        RuvioSocket connection = await ConnectionAsync(host, port, cancellationToken);
        RespValue reply = await connection.SendAsync(forwarded, cancellationToken);

        if (reply.Kind is RespKind.Error || reply.Items is not [var next, var keys])
        {
            return reply;
        }

        ulong serverNext = ulong.Parse(next.AsString() ?? "0", NumberStyles.None, CultureInfo.InvariantCulture);
        ulong clientNext = serverNext != 0
            ? checked((serverNext * nodes) + (ulong)node)
            : (ulong)node + 1 < nodes ? (ulong)node + 1 : 0;

        byte[] encoded = Encoding.ASCII.GetBytes(clientNext.ToString(CultureInfo.InvariantCulture));

        return RespValue.Array([RespValue.Bulk(encoded), keys]);
    }

    /// <summary>Sends one sub-command per slot, pipelined per shard, and merges the replies.</summary>
    private async Task<RespValue> SplitAsync(
        string name,
        IReadOnlyList<string> arguments,
        IReadOnlyList<string> keys,
        CancellationToken cancellationToken)
    {
        bool pairs = name == "MSET";
        var bySlot = new Dictionary<int, List<int>>();

        for (int index = 0; index < keys.Count; index++)
        {
            int slot = ClusterKeys.Slot(keys[index]);

            if (!bySlot.TryGetValue(slot, out List<int>? positions))
            {
                positions = [];
                bySlot[slot] = positions;
            }

            positions.Add(index);
        }

        var parts = new List<(int Slot, List<int> Positions, string[] Command)>();

        foreach ((int slot, List<int> positions) in bySlot)
        {
            var command = new string[1 + (positions.Count * (pairs ? 2 : 1))];
            int written = 0;

            command[written++] = arguments[0];

            foreach (int position in positions)
            {
                if (pairs)
                {
                    command[written++] = arguments[1 + (position * 2)];
                    command[written++] = arguments[2 + (position * 2)];
                }
                else
                {
                    command[written++] = keys[position];
                }
            }

            parts.Add((slot, positions, command));
        }

        var byConnection = new Dictionary<RuvioSocket, List<int>>();

        for (int part = 0; part < parts.Count; part++)
        {
            RuvioSocket owner = await ForSlotAsync(parts[part].Slot, cancellationToken);

            if (!byConnection.TryGetValue(owner, out List<int>? indexes))
            {
                indexes = [];
                byConnection[owner] = indexes;
            }

            indexes.Add(part);
        }

        var replies = new RespValue[parts.Count];

        var sends = new Task[byConnection.Count];
        int sent = 0;

        foreach ((RuvioSocket connection, List<int> indexes) in byConnection)
        {
            var batch = new IReadOnlyList<string>[indexes.Count];

            for (int index = 0; index < batch.Length; index++)
            {
                batch[index] = parts[indexes[index]].Command;
            }

            sends[sent++] = SendGroupAsync(connection, batch, indexes, replies, cancellationToken);
        }

        await Task.WhenAll(sends);

        for (int part = 0; part < parts.Count; part++)
        {
            if (MovedTarget(replies[part]) is { } node)
            {
                await RefreshAsync(cancellationToken);
                RuvioSocket owner = await ConnectionAsync(node.Host, node.Port, cancellationToken);

                replies[part] = await SendRoutedAsync(owner, parts[part].Command, cancellationToken);
            }

            if (replies[part].Kind is RespKind.Error)
            {
                return replies[part];
            }
        }

        switch (name)
        {
            case "MSET":
                return RespValue.Simple("OK");

            case "MGET":
                var values = new RespValue[keys.Count];

                for (int part = 0; part < parts.Count; part++)
                {
                    IReadOnlyList<RespValue> items = replies[part].Items ?? [];

                    for (int index = 0; index < items.Count; index++)
                    {
                        values[parts[part].Positions[index]] = items[index];
                    }
                }

                return RespValue.Array(values);

            default:
                return RespValue.FromInteger(replies.Sum(reply => reply.Integer));
        }
    }

    private static bool HasTransactionCommand(IReadOnlyList<IReadOnlyList<string>> commands)
    {
        for (int index = 0; index < commands.Count; index++)
        {
            if (TransactionCommands.Contains(ClusterKeys.Name(commands[index])))
            {
                return true;
            }
        }

        return false;
    }

    private static async Task SendGroupAsync(
        RuvioSocket connection,
        IReadOnlyList<IReadOnlyList<string>> batch,
        List<int> positions,
        RespValue[] replies,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<RespValue> values = await connection.SendManyAsync(batch, cancellationToken);

        for (int index = 0; index < values.Count; index++)
        {
            replies[positions[index]] = values[index];
        }
    }

    private static bool SpansSlots(IReadOnlyList<string> keys)
    {
        if (keys.Count < 2)
        {
            return false;
        }

        int first = ClusterKeys.Slot(keys[0]);

        for (int index = 1; index < keys.Count; index++)
        {
            if (ClusterKeys.Slot(keys[index]) != first)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Moves a key whose destination lives on another shard. One shard cannot
    /// rename into a slot it does not own, so the client copies the value and
    /// then deletes the source.
    /// </summary>
    private async Task<RespValue> RenameAcrossAsync(
        bool exclusive,
        string source,
        string destination,
        CancellationToken cancellationToken)
    {
        if (exclusive)
        {
            RespValue exists = await CallAsync(["EXISTS", destination], cancellationToken);

            if (exists.Kind == RespKind.Error) return exists;

            if (exists.Kind == RespKind.Integer && exists.Integer == 1) return RespValue.FromInteger(0);
        }

        RespValue type = await CallAsync(["TYPE", source], cancellationToken);

        if (type.Kind == RespKind.Error) return type;
        string kind = type.AsString() ?? "none";

        if (kind == "none") return RespValue.Error("ERR no such key");

        RespValue ttlReply = await CallAsync(["PTTL", source], cancellationToken);

        if (ttlReply.Kind == RespKind.Error) return ttlReply;
        long ttl = ttlReply.Kind == RespKind.Integer ? ttlReply.Integer : -1;
        Moved moved = await ReadForRenameAsync(kind, source, cancellationToken);

        RespValue deleted = await CallAsync(["DEL", destination], cancellationToken);

        if (deleted.Kind == RespKind.Error) return deleted;
        await WriteRenamedAsync(kind, destination, moved, cancellationToken);

        if (ttl > 0)
        {
            RespValue expire = await CallAsync(["PEXPIRE", destination, ttl.ToString(CultureInfo.InvariantCulture)], cancellationToken);

            if (expire.Kind == RespKind.Error) return expire;
        }

        return await CallAsync(["DEL", source], cancellationToken) is { Kind: RespKind.Error } removal
            ? removal
            : exclusive ? RespValue.FromInteger(1) : RespValue.Simple("OK");
    }

    private async Task<Moved> ReadForRenameAsync(string kind, string source, CancellationToken cancellationToken)
    {
        switch (kind)
        {
            case "string":
                RespValue value = await CallAsync(["GET", source], cancellationToken);

                if (value.Kind == RespKind.Error) throw new RuvioException(value.Text ?? "GET failed", isServerReply: true);

                return new Moved.Text(value.AsString() ?? "");
            case "list":
                return new Moved.Values(await ReadRangeAsync("LRANGE", source, [], cancellationToken));
            case "set":
                RespValue members = await CallAsync(["SMEMBERS", source], cancellationToken);

                if (members.Kind == RespKind.Error) throw new RuvioException(members.Text ?? "SMEMBERS failed", isServerReply: true);

                return new Moved.Values(Texts(members));
            case "hash":
                return new Moved.Pairs(await ReadHashAsync(source, cancellationToken));
            case "zset":
                return new Moved.Pairs(PairUp(await ReadRangeAsync("ZRANGE", source, ["WITHSCORES"], cancellationToken)));
            case "stream":
                return new Moved.StreamEntries(await ReadStreamAsync(source, cancellationToken));
            default:
                throw new RuvioException($"cross-slot rename does not move a {kind} value");
        }
    }

    private async Task WriteRenamedAsync(string kind, string destination, Moved moved, CancellationToken cancellationToken)
    {
        switch (kind, moved)
        {
            case ("string", Moved.Text text):
                RespValue set = await CallAsync(["SET", destination, text.Value], cancellationToken);

                if (set.Kind == RespKind.Error) throw new RuvioException(set.Text ?? "SET failed", isServerReply: true);
                break;
            case ("list", Moved.Values values):
                await PushManyAsync("RPUSH", destination, values.Items, cancellationToken);
                break;
            case ("set", Moved.Values values):
                await PushManyAsync("SADD", destination, values.Items, cancellationToken);
                break;
            case ("hash", Moved.Pairs pairs):
                await PushPairsAsync("HSET", destination, pairs.Items, scoreFirst: false, cancellationToken);
                break;
            case ("zset", Moved.Pairs pairs):
                await PushPairsAsync("ZADD", destination, pairs.Items, scoreFirst: true, cancellationToken);
                break;
            case ("stream", Moved.StreamEntries entries):
                foreach ((string id, List<(string Field, string Value)> fields) in entries.Items)
                {
                    var arguments = new List<string> { "XADD", destination, id };

                    foreach ((string field, string value) in fields)
                    {
                        arguments.Add(field);
                        arguments.Add(value);
                    }

                    if (arguments.Count > 3)
                    {
                        RespValue added = await CallAsync([.. arguments], cancellationToken);

                        if (added.Kind == RespKind.Error) throw new RuvioException(added.Text ?? "XADD failed", isServerReply: true);
                    }
                }

                break;
            default:
                throw new RuvioException("cross-slot rename lost the value shape");
        }
    }

    private async Task<List<string>> ReadRangeAsync(
        string command,
        string source,
        string[] extra,
        CancellationToken cancellationToken)
    {
        var values = new List<string>();

        for (int start = 0; ; start += 200)
        {
            var arguments = new List<string> { command, source, start.ToString(CultureInfo.InvariantCulture), (start + 199).ToString(CultureInfo.InvariantCulture) };

            arguments.AddRange(extra);
            RespValue page = await CallAsync([.. arguments], cancellationToken);

            if (page.Kind == RespKind.Error) throw new RuvioException(page.Text ?? command + " failed", isServerReply: true);
            List<string> chunk = Texts(page);

            values.AddRange(chunk);

            if (chunk.Count < 200) break;
        }

        return values;
    }

    private async Task<List<(string Left, string Right)>> ReadHashAsync(string source, CancellationToken cancellationToken)
    {
        string cursor = "0";
        var pairs = new List<(string, string)>();

        while (true)
        {
            RespValue page = await CallAsync(["HSCAN", source, cursor, "COUNT", "64"], cancellationToken);

            if (page.Kind == RespKind.Error)
                throw new RuvioException(page.Text ?? "HSCAN failed", isServerReply: true);

            IReadOnlyList<RespValue> items = page.Items ?? [];
            string next = items.Count > 0 ? items[0].AsString() ?? "0" : "0";

            if (items.Count > 1)
                pairs.AddRange(PairUp(Texts(items[1])));

            if (next == "0")
                break;

            cursor = next;
        }

        return pairs;
    }

    private async Task<List<(string Id, List<(string Field, string Value)> Fields)>> ReadStreamAsync(
        string source,
        CancellationToken cancellationToken)
    {
        string start = "-";
        var entries = new List<(string, List<(string, string)>)>();

        while (true)
        {
            RespValue page = await CallAsync(["XRANGE", source, start, "+", "COUNT", "64"], cancellationToken);

            if (page.Kind == RespKind.Error)
                throw new RuvioException(page.Text ?? "XRANGE failed", isServerReply: true);

            var fresh = new List<(string, List<(string, string)>)>();

            foreach (RespValue entry in page.Items ?? [])
            {
                IReadOnlyList<RespValue> parts = entry.Items ?? [];
                string id = parts.Count > 0 ? parts[0].AsString() ?? "" : "";

                if (id.Length == 0 || id == start)
                    continue;

                List<(string, string)> fields = parts.Count > 1 ? PairUp(Texts(parts[1])) : [];

                fresh.Add((id, fields));
            }

            if (fresh.Count == 0)
                break;

            start = fresh[^1].Item1;
            bool done = fresh.Count < 63;

            entries.AddRange(fresh);

            if (done)
                break;
        }

        return entries;
    }

    private async Task PushManyAsync(string command, string key, List<string> values, CancellationToken cancellationToken)
    {
        for (int offset = 0; offset < values.Count; offset += 32)
        {
            var arguments = new List<string> { command, key };

            arguments.AddRange(values.Skip(offset).Take(32));
            RespValue reply = await CallAsync([.. arguments], cancellationToken);

            if (reply.Kind == RespKind.Error)
                throw new RuvioException(reply.Text ?? command + " failed", isServerReply: true);
        }
    }

    private async Task PushPairsAsync(
        string command,
        string key,
        List<(string Left, string Right)> pairs,
        bool scoreFirst,
        CancellationToken cancellationToken)
    {
        for (int offset = 0; offset < pairs.Count; offset += 32)
        {
            var arguments = new List<string> { command, key };

            foreach ((string left, string right) in pairs.Skip(offset).Take(32))
            {
                arguments.Add(scoreFirst ? right : left);
                arguments.Add(scoreFirst ? left : right);
            }

            RespValue reply = await CallAsync([.. arguments], cancellationToken);

            if (reply.Kind == RespKind.Error)
                throw new RuvioException(reply.Text ?? command + " failed", isServerReply: true);
        }
    }

    private async Task<RespValue> CallAsync(string[] arguments, CancellationToken cancellationToken)
    {
        RuvioSocket connection = await TargetKeyAsync(ClusterKeys.First(ClusterKeys.Name(arguments), arguments), cancellationToken);

        return await SendRoutedAsync(connection, arguments, cancellationToken);
    }

    private static List<string> Texts(RespValue value)
    {
        if (value.Items is not { } items) return [];

        return [.. items.Select(item => item.AsString() ?? "")];
    }

    private static List<(string Left, string Right)> PairUp(List<string> flat)
    {
        var pairs = new List<(string, string)>();

        for (int index = 0; index < flat.Count; index += 2)
        {
            pairs.Add((flat[index], index + 1 < flat.Count ? flat[index + 1] : ""));
        }

        return pairs;
    }

    private abstract record Moved
    {
        public sealed record Text(string Value) : Moved;
        public sealed record Values(List<string> Items) : Moved;
        public sealed record Pairs(List<(string Left, string Right)> Items) : Moved;
        public sealed record StreamEntries(List<(string Id, List<(string Field, string Value)> Fields)> Items) : Moved;
    }

    private async Task<RespValue> SendRoutedAsync(
        RuvioSocket connection,
        IReadOnlyList<string> arguments,
        CancellationToken cancellationToken)
    {
        for (int redirect = 0; ; redirect++)
        {
            RespValue reply = await connection.SendAsync(arguments, cancellationToken);

            if (redirect >= MaxRedirects || MovedTarget(reply) is not { } node)
            {
                return reply;
            }

            // MOVED means the slot map is stale: refresh it, then connect to the node the reply names.
            await RefreshAsync(cancellationToken);
            connection = await ConnectionAsync(node.Host, node.Port, cancellationToken);
        }
    }

    internal static (string Host, int Port)? MovedTarget(RespValue reply)
    {
        if (reply.Kind is not RespKind.Error || reply.Text is not { } text || !text.StartsWith("MOVED ", StringComparison.Ordinal))
        {
            return null;
        }

        string[] parts = text.Split(' ');

        if (parts.Length < 3)
        {
            return null;
        }

        int colon = parts[2].LastIndexOf(':');

        if (colon <= 0
            || !int.TryParse(parts[2][(colon + 1)..], NumberStyles.None, CultureInfo.InvariantCulture, out int port))
        {
            return null;
        }

        return (parts[2][..colon], port);
    }

    private async Task RefreshAsync(CancellationToken cancellationToken)
    {
        await _refreshGate.WaitAsync(cancellationToken);

        try
        {
            RuvioSocket[] live;

            lock (_connections)
            {
                live = [.. _connections.Values.Where(connection => !connection.IsBroken)];
            }

            foreach (RuvioSocket connection in live)
            {
                RespValue reply = await connection.SendAsync(["CLUSTER", "SLOTS"], cancellationToken);

                if (ClusterTopology.Parse(reply) is { } topology)
                {
                    _topology = topology;

                    return;
                }
            }
        }

        finally
        {
            _refreshGate.Release();
        }
    }

    private ValueTask<RuvioSocket> TargetAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken)
    {
        return TargetKeyAsync(keys.Count > 0 ? keys[0] : null, cancellationToken);
    }

    private ValueTask<RuvioSocket> TargetKeyAsync(string? key, CancellationToken cancellationToken)
    {
        if (key is not null)
        {
            return ForSlotAsync(ClusterKeys.Slot(key), cancellationToken);
        }

        return new ValueTask<RuvioSocket>(ConnectionAsync(_seed.Host, _seed.Port, cancellationToken));
    }

    private async Task<RuvioSocket> FirstKeyedTargetAsync(
        IReadOnlyList<IReadOnlyList<string>> commands,
        CancellationToken cancellationToken)
    {
        foreach (IReadOnlyList<string> command in commands)
        {
            if (ClusterKeys.First(ClusterKeys.Name(command), command) is { } key)
            {
                return await ForSlotAsync(ClusterKeys.Slot(key), cancellationToken);
            }
        }

        return await TargetKeyAsync(null, cancellationToken);
    }

    private ValueTask<RuvioSocket> ForSlotAsync(int slot, CancellationToken cancellationToken)
    {
        ClusterTopology topology = _topology;
        int node = topology.NodeOf(slot);

        if (node < 0)
        {
            return TargetKeyAsync(null, cancellationToken);
        }

        if (topology.Sockets[node] is { IsBroken: false } cached)
        {
            return new ValueTask<RuvioSocket>(cached);
        }

        return OpenForNodeAsync(topology, node, cancellationToken);
    }

    private async ValueTask<RuvioSocket> OpenForNodeAsync(
        ClusterTopology topology, int node, CancellationToken cancellationToken)
    {
        (string host, int port) = topology.Nodes[node];
        RuvioSocket connection = await ConnectionAsync(host, port, cancellationToken);

        topology.Sockets[node] = connection;

        return connection;
    }

    private async Task<RuvioSocket[]> EveryConnectionAsync(CancellationToken cancellationToken)
    {
        var connections = new List<RuvioSocket>();

        foreach ((string host, int port) in _topology.Nodes)
        {
            connections.Add(await ConnectionAsync(host, port, cancellationToken));
        }

        return [.. connections];
    }

    private async Task<RuvioSocket> ConnectionAsync(
        string host,
        int port,
        CancellationToken cancellationToken,
        bool force = false)
    {
        string endpoint = Endpoint(host, port);

        lock (_connections)
        {
            if (_connections.TryGetValue(endpoint, out RuvioSocket? existing) && !existing.IsBroken)
            {
                return existing;
            }
        }

        await _connectGate.WaitAsync(cancellationToken);

        try
        {
            RuvioSocket? stale;

            lock (_connections)
            {
                if (_connections.TryGetValue(endpoint, out stale) && !stale.IsBroken)
                {
                    return stale;
                }
            }

            ObjectDisposedException.ThrowIf(_disposed, this);

            if (stale is { IsBroken: true } && !_options.Reconnect && !force)
            {
                throw new RuvioException("connection is closed");
            }

            if (stale is { IsBroken: true })
            {
                _lost?.Invoke(host, port, new RuvioException("connection is closed"));
            }

            RuvioSocket opened = _open is null
                ? await RuvioSocket.OpenAsync(host, port, _options, cancellationToken)
                : await _open(host, port, cancellationToken);

            if (stale is { IsBroken: true })
            {
                _restored?.Invoke(host, port);
            }

            lock (_connections)
            {
                _connections[endpoint] = opened;
            }

            if (stale is not null && !ReferenceEquals(stale, _seed))
            {
                stale.Dispose();
            }

            return opened;
        }

        finally
        {
            _connectGate.Release();
        }
    }

    private static string Endpoint(string host, int port)
    {
        return $"{host}:{port.ToString(CultureInfo.InvariantCulture)}";
    }

    public void Dispose()
    {
        _disposed = true;

        lock (_connections)
        {
            foreach (RuvioSocket connection in _connections.Values)
            {
                connection.Dispose();
            }

            _connections.Clear();
        }
    }
}

/// <summary>Which node owns each slot, and the nodes in slot order.</summary>
internal sealed class ClusterTopology
{
    private readonly short[] _owners;

    private ClusterTopology(short[] owners, IReadOnlyList<(string Host, int Port)> nodes)
    {
        _owners = owners;
        Nodes = nodes;
    }

    public IReadOnlyList<(string Host, int Port)> Nodes { get; }

    /// <summary>Open sockets by node index, so a warm route needs no endpoint string or lookup lock.</summary>
    internal RuvioSocket?[] Sockets { get; private init; } = [];

    public (string Host, int Port)? Owner(int slot)
    {
        short node = _owners[slot];

        return node < 0 ? null : Nodes[node];
    }

    public int NodeOf(int slot) => _owners[slot];

    /// <summary>Reads a <c>CLUSTER SLOTS</c> reply. Null for an error or an empty map.</summary>
    public static ClusterTopology? Parse(RespValue reply)
    {
        if (reply.Kind is not RespKind.Array || reply.Items is not { Count: > 0 } ranges)
        {
            return null;
        }

        var owners = new short[ClusterKeys.SlotCount];

        Array.Fill(owners, (short)-1);

        var nodes = new List<(string Host, int Port)>();

        foreach (RespValue range in ranges.OrderBy(range => range.Items?[0].Integer ?? 0))
        {
            if (range.Items is not { Count: >= 3 } fields || fields[2].Items is not { Count: >= 2 } primary)
            {
                return null;
            }

            string host = primary[0].AsString() ?? "";
            int port = (int)primary[1].Integer;
            int node = nodes.FindIndex(existing =>
                existing.Port == port && string.Equals(existing.Host, host, StringComparison.OrdinalIgnoreCase));

            if (node < 0)
            {
                node = nodes.Count;
                nodes.Add((host, port));
            }

            long first = Math.Max(0, fields[0].Integer);
            long last = Math.Min(ClusterKeys.SlotCount - 1, fields[1].Integer);

            for (long slot = first; slot <= last; slot++)
            {
                owners[slot] = (short)node;
            }
        }

        return new ClusterTopology(owners, nodes) { Sockets = new RuvioSocket?[nodes.Count] };
    }
}
