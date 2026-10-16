using System.Runtime.CompilerServices;
using System.Globalization;
using System.Text;

namespace Ruvio.Client;

/// <summary>
/// Routes commands to the shard that owns their slot. Built from <c>CLUSTER SLOTS</c>;
/// a <c>MOVED</c> reply points that slot at the named node and retries, at most three times.
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

    /// <summary>
    /// Routes one command. <paramref name="redirects"/> counts the <c>MOVED</c> replies the
    /// caller already followed for it.
    /// </summary>
    public async Task<RespValue> ExecuteAsync(
        IReadOnlyList<string> arguments,
        CancellationToken cancellationToken,
        int redirects = 0)
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

        if (SpansSlots(keys) && !UnpairedMset(name, arguments))
        {
            // Only the split commands can be answered per slot; anything else (RENAME, COPY,
            // SMOVE, EVAL, ...) is refused here instead of reaching a shard.
            return Split.Contains(name)
                ? await SplitAsync(name, arguments, keys, cancellationToken)
                : CrossSlot();
        }

        RuvioSocket connection = await TargetAsync(keys, cancellationToken);

        return await SendRoutedAsync(connection, arguments, cancellationToken, redirects);
    }

    /// <summary>
    /// Routes an ordinary single-key command without building a string list. Everything
    /// else (transactions, multi-key, fan-out) takes the list path.
    /// </summary>
    internal ValueTask<RespValue> ExecuteAsync(in RespCommand command, CancellationToken cancellationToken, int redirects = 0)
    {
        if (_pinned is null && !_multiPending && command.Count >= 2
            && command.Name is { } raw && command.TextAt(1) is { } key)
        {
            string name = raw.ToUpperInvariant();

            if (ClusterKeys.IsSingleKey(name))
            {
                return SendToSlotAsync(command, ClusterKeys.Slot(key), redirects, cancellationToken);
            }
        }

        return new ValueTask<RespValue>(ExecuteAsync(command.AsList(), cancellationToken, redirects));
    }

    /// <summary>The error for keys in several slots, returned before anything is sent.</summary>
    internal static RespValue CrossSlot()
    {
        return RespValue.Error("CROSSSLOT Keys in request do not hash to the same slot (checked by the client, nothing was sent)");
    }

    // Pooled builder: each routed call reuses a state machine box instead of allocating a new one.
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<RespValue> SendToSlotAsync(
        RespCommand command, int slot, int redirects, CancellationToken cancellationToken)
    {
        RuvioSocket connection = await ForSlotAsync(slot, cancellationToken).ConfigureAwait(false);

        for (int redirect = redirects; ; redirect++)
        {
            RespValue reply = await connection.SendAsync(command, cancellationToken).ConfigureAwait(false);

            if (redirect >= MaxRedirects || MovedTarget(reply) is not { } moved)
            {
                return reply;
            }

            connection = await FollowMovedAsync(moved, redirect, cancellationToken).ConfigureAwait(false);
        }
    }

    internal async Task<RespValue> ExecuteBinaryAsync(
        IReadOnlyList<ReadOnlyMemory<byte>> arguments, CancellationToken cancellationToken, int redirects = 0)
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
                // DEL, UNLINK and EXISTS are split like their string form; a script cannot be.
                return first == 1
                    ? await SplitBinaryAsync(arguments, cancellationToken).ConfigureAwait(false)
                    : CrossSlot();
            }
        }

        return await SendBinaryToSlotAsync(arguments, slot, redirects, cancellationToken).ConfigureAwait(false);
    }

    private async Task<RespValue> SendBinaryToSlotAsync(
        IReadOnlyList<ReadOnlyMemory<byte>> arguments, int slot, int redirects, CancellationToken cancellationToken)
    {
        RuvioSocket connection = await ForSlotAsync(slot, cancellationToken).ConfigureAwait(false);

        for (int redirect = redirects; ; redirect++)
        {
            RespValue reply = await connection.SendBinaryAsync(arguments, cancellationToken).ConfigureAwait(false);

            if (redirect >= MaxRedirects || MovedTarget(reply) is not { } moved)
            {
                return reply;
            }

            connection = await FollowMovedAsync(moved, redirect, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Sends a binary <c>DEL</c>, <c>UNLINK</c> or <c>EXISTS</c> once per slot and sums the counts.</summary>
    private async Task<RespValue> SplitBinaryAsync(
        IReadOnlyList<ReadOnlyMemory<byte>> arguments, CancellationToken cancellationToken)
    {
        var bySlot = new Dictionary<int, List<ReadOnlyMemory<byte>>>();

        for (int index = 1; index < arguments.Count; index++)
        {
            int slot = ClusterKeys.Slot(arguments[index].Span);

            if (!bySlot.TryGetValue(slot, out List<ReadOnlyMemory<byte>>? part))
            {
                part = [arguments[0]];
                bySlot[slot] = part;
            }

            part.Add(arguments[index]);
        }

        var sends = new Task<RespValue>[bySlot.Count];
        int next = 0;

        foreach ((int slot, List<ReadOnlyMemory<byte>> part) in bySlot)
        {
            sends[next++] = SendBinaryToSlotAsync(part, slot, 0, cancellationToken);
        }

        long total = 0;

        foreach (RespValue reply in await Task.WhenAll(sends).ConfigureAwait(false))
        {
            if (reply.Kind is RespKind.Error)
            {
                return reply;
            }

            total += reply.Integer;
        }

        return RespValue.FromInteger(total);
    }

    /// <summary>
    /// Groups a pipeline by shard and sends each group as one write. Replies come back in
    /// command order. A split command whose keys span slots is sent per slot inside the
    /// groups and its replies merged at its own index. A pipeline that holds a transaction
    /// goes to one shard unchanged.
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

        // Per-slot parts of split commands get positions after the pipeline's own commands.
        List<IReadOnlyList<string>>? extra = null;
        List<(int Index, string Name, int Keys, List<SplitPart> Parts, int Offset)>? splits = null;

        for (int index = 0; index < commands.Count; index++)
        {
            IReadOnlyList<string> command = commands[index];
            string name = ClusterKeys.Name(command);

            if (Split.Contains(name) && SplitSpansSlots(name, command))
            {
                IReadOnlyList<string> keys = ClusterKeys.Of(name, command);
                List<SplitPart> parts = SplitParts(name, command, keys);

                extra ??= [];
                splits ??= [];
                splits.Add((index, name, keys.Count, parts, commands.Count + extra.Count));

                foreach (SplitPart part in parts)
                {
                    AddToGroup(groups, await ForSlotAsync(part.Slot, cancellationToken), commands.Count + extra.Count);
                    extra.Add(part.Command);
                }

                continue;
            }

            AddToGroup(groups, await TargetKeyAsync(ClusterKeys.First(name, command), cancellationToken), index);
        }

        var replies = new RespValue[commands.Count + (extra?.Count ?? 0)];

        if (groups.Count == 1 && extra is null)
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
                    int position = positions[index];

                    batch[index] = position < commands.Count ? commands[position] : extra![position - commands.Count];
                }

                sends[next++] = SendGroupAsync(connection, batch, positions, replies, cancellationToken);
            }

            await Task.WhenAll(sends);
        }

        for (int index = 0; index < replies.Length; index++)
        {
            // A split command's own index stays empty until its parts are merged below.
            if (replies[index] is { } reply && MovedTarget(reply) is { } moved)
            {
                IReadOnlyList<string> command = index < commands.Count ? commands[index] : extra![index - commands.Count];
                RuvioSocket owner = await FollowMovedAsync(moved, 0, cancellationToken);

                replies[index] = await SendRoutedAsync(owner, command, cancellationToken, redirects: 1);
            }
        }

        if (splits is null)
        {
            return replies;
        }

        foreach ((int index, string name, int keys, List<SplitPart> parts, int offset) in splits)
        {
            replies[index] = MergeSplit(name, keys, parts, replies, offset);
        }

        return replies[..commands.Count];
    }

    private static void AddToGroup(Dictionary<RuvioSocket, List<int>> groups, RuvioSocket connection, int position)
    {
        if (!groups.TryGetValue(connection, out List<int>? positions))
        {
            positions = [];
            groups[connection] = positions;
        }

        positions.Add(position);
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
        List<SplitPart> parts = SplitParts(name, arguments, keys);
        var byConnection = new Dictionary<RuvioSocket, List<int>>();

        for (int part = 0; part < parts.Count; part++)
        {
            AddToGroup(byConnection, await ForSlotAsync(parts[part].Slot, cancellationToken), part);
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
            if (MovedTarget(replies[part]) is { } moved)
            {
                RuvioSocket owner = await FollowMovedAsync(moved, 0, cancellationToken);

                replies[part] = await SendRoutedAsync(owner, parts[part].Command, cancellationToken, redirects: 1);
            }
        }

        return MergeSplit(name, keys.Count, parts, replies, 0);
    }

    /// <summary>One slot's share of a split command: its key positions and the command to send.</summary>
    private readonly record struct SplitPart(int Slot, List<int> Positions, string[] Command);

    /// <summary>Breaks a split command into one command per slot, keys in their original order.</summary>
    private static List<SplitPart> SplitParts(string name, IReadOnlyList<string> arguments, IReadOnlyList<string> keys)
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

        var parts = new List<SplitPart>(bySlot.Count);

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

            parts.Add(new SplitPart(slot, positions, command));
        }

        return parts;
    }

    /// <summary>
    /// Merges the part replies at <paramref name="offset"/>: <c>MGET</c> in key order, <c>MSET</c>
    /// as OK, counts summed. The first error reply wins.
    /// </summary>
    private static RespValue MergeSplit(string name, int keyCount, List<SplitPart> parts, RespValue[] replies, int offset)
    {
        for (int part = 0; part < parts.Count; part++)
        {
            if (replies[offset + part].Kind is RespKind.Error)
            {
                return replies[offset + part];
            }
        }

        switch (name)
        {
            case "MSET":
                return RespValue.Simple("OK");

            case "MGET":
                var values = new RespValue[keyCount];

                for (int part = 0; part < parts.Count; part++)
                {
                    IReadOnlyList<RespValue> items = replies[offset + part].Items ?? [];

                    for (int index = 0; index < items.Count; index++)
                    {
                        values[parts[part].Positions[index]] = items[index];
                    }
                }

                return RespValue.Array(values);

            default:
                long total = 0;

                for (int part = 0; part < parts.Count; part++)
                {
                    total += replies[offset + part].Integer;
                }

                return RespValue.FromInteger(total);
        }
    }

    /// <summary>
    /// True when a split command's keys span slots, read straight from the arguments so a
    /// one-slot <c>MGET</c> in a pipeline builds no key list.
    /// </summary>
    private static bool SplitSpansSlots(string name, IReadOnlyList<string> command)
    {
        int step = name == "MSET" ? 2 : 1;

        if (command.Count < 2 + step || UnpairedMset(name, command))
        {
            return false;
        }

        int first = ClusterKeys.Slot(command[1]);

        for (int index = 1 + step; index < command.Count; index += step)
        {
            if (ClusterKeys.Slot(command[index]) != first)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>An <c>MSET</c> with a key and no value is not split; the shard answers with its arity error.</summary>
    private static bool UnpairedMset(string name, IReadOnlyList<string> arguments) =>
        name == "MSET" && arguments.Count % 2 == 0;

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

    private async Task<RespValue> SendRoutedAsync(
        RuvioSocket connection,
        IReadOnlyList<string> arguments,
        CancellationToken cancellationToken,
        int redirects = 0)
    {
        for (int redirect = redirects; ; redirect++)
        {
            RespValue reply = await connection.SendAsync(arguments, cancellationToken);

            if (redirect >= MaxRedirects || MovedTarget(reply) is not { } moved)
            {
                return reply;
            }

            connection = await FollowMovedAsync(moved, redirect, cancellationToken);
        }
    }

    /// <summary>
    /// Points the slot a <c>MOVED</c> names at its node and returns that node's socket; the
    /// retry always goes to the named node. <paramref name="redirect"/> counts the earlier
    /// <c>MOVED</c> replies for the same command. The whole slot map is read again when the
    /// node is not in it, and on the command's second and later <c>MOVED</c>.
    /// </summary>
    private async Task<RuvioSocket> FollowMovedAsync(
        (int Slot, string Host, int Port) moved, int redirect, CancellationToken cancellationToken)
    {
        if (redirect > 0 || !_topology.Assign(moved.Slot, moved.Host, moved.Port))
        {
            await RefreshAsync(cancellationToken).ConfigureAwait(false);
            _topology.Assign(moved.Slot, moved.Host, moved.Port);
        }

        return await ConnectionAsync(moved.Host, moved.Port, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Points one slot at the node a <c>MOVED</c> named, when that node is in the map.</summary>
    internal void PointSlot((int Slot, string Host, int Port) moved)
    {
        _topology.Assign(moved.Slot, moved.Host, moved.Port);
    }

    /// <summary>The slot and node of a <c>MOVED slot host:port</c> reply; null for any other reply.</summary>
    internal static (int Slot, string Host, int Port)? MovedTarget(RespValue reply)
    {
        if (reply.Kind is not RespKind.Error || reply.Text is not { } text || !text.StartsWith("MOVED ", StringComparison.Ordinal))
        {
            return null;
        }

        string[] parts = text.Split(' ');

        if (parts.Length < 3
            || !int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out int slot))
        {
            return null;
        }

        int colon = parts[2].LastIndexOf(':');

        if (colon <= 0
            || !int.TryParse(parts[2][(colon + 1)..], NumberStyles.None, CultureInfo.InvariantCulture, out int port))
        {
            return null;
        }

        return (slot, parts[2][..colon], port);
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

    /// <summary>
    /// Points one slot at a node already in the map, after a <c>MOVED</c>. False when the slot
    /// is out of range or the node is unknown, so the caller reads the whole map instead.
    /// </summary>
    public bool Assign(int slot, string host, int port)
    {
        if ((uint)slot >= ClusterKeys.SlotCount)
        {
            return false;
        }

        for (int node = 0; node < Nodes.Count; node++)
        {
            if (Nodes[node].Port == port && string.Equals(Nodes[node].Host, host, StringComparison.OrdinalIgnoreCase))
            {
                _owners[slot] = (short)node;

                return true;
            }
        }

        return false;
    }

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
