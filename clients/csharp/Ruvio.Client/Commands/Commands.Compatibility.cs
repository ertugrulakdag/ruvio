using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;

namespace Ruvio.Client;

public sealed partial class RuvioClient
{
    /// <summary>Sends <c>KEYS</c>. The server refuses to return more than 100,000 keys.</summary>
    public ValueTask<IReadOnlyList<string>> GetKeysAsync(string pattern, CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("KEYS", pattern), cancellationToken);
    }

    /// <summary>Sends <c>RANDOMKEY</c>. Null when the database is empty.</summary>
    public ValueTask<string?> GetRandomKeyAsync(CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("RANDOMKEY"), cancellationToken);
    }

    /// <summary>Sends <c>TOUCH</c> and returns how many of the keys exist.</summary>
    public ValueTask<long> TouchAsync(params string[] keys)
    {
        return Integer(Join("TOUCH", keys));
    }

    /// <summary>Sends <c>RENAMENX</c>. False when the new key already exists.</summary>
    public ValueTask<bool> RenameIfNotExistsAsync(string key, string newKey, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("RENAMENX", key, newKey), cancellationToken);
    }

    /// <summary>Sends <c>EXPIRETIME</c>: unix seconds, -1 without a TTL, -2 for a missing key.</summary>
    public ValueTask<long> GetExpireTimeAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("EXPIRETIME", key), cancellationToken);
    }

    /// <summary>Sends <c>PEXPIRETIME</c>: unix milliseconds, -1 without a TTL, -2 for a missing key.</summary>
    public ValueTask<long> GetExpireTimeMillisecondsAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("PEXPIRETIME", key), cancellationToken);
    }

    /// <summary>Sends <c>COPY</c>. False when the source is missing or the target exists and <paramref name="replace"/> is false.</summary>
    public ValueTask<bool> CopyAsync(string source, string destination, bool replace = false, CancellationToken cancellationToken = default)
    {
        return replace
            ? Flag(new RespCommand("COPY", source, destination, "REPLACE"), cancellationToken)
            : Flag(new RespCommand("COPY", source, destination), cancellationToken);
    }

    /// <summary>Sends <c>ECHO</c>.</summary>
    public async ValueTask<string> EchoAsync(string message, CancellationToken cancellationToken = default)
    {
        return (await ExecuteCommandAsync(new RespCommand("ECHO", message), cancellationToken).ConfigureAwait(false)).AsString()
            ?? throw new RuvioException("ECHO returned a null reply");
    }

    /// <summary>Sends <c>OBJECT ENCODING</c>. Null for a missing key.</summary>
    public ValueTask<string?> GetObjectEncodingAsync(string key, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("OBJECT", "ENCODING", key), cancellationToken);
    }

    /// <summary>Sends <c>OBJECT IDLETIME</c> in seconds. Null for a missing key.</summary>
    public ValueTask<long?> GetObjectIdleSecondsAsync(string key, CancellationToken cancellationToken = default)
    {
        return OptionalInteger(new RespCommand("OBJECT", "IDLETIME", key), cancellationToken);
    }

    /// <summary>Sends <c>OBJECT FREQ</c>. Null for a missing key.</summary>
    public ValueTask<long?> GetObjectFrequencyAsync(string key, CancellationToken cancellationToken = default)
    {
        return OptionalInteger(new RespCommand("OBJECT", "FREQ", key), cancellationToken);
    }

    /// <summary>Sends <c>MEMORY USAGE</c>. Null for a missing key.</summary>
    public ValueTask<long?> GetMemoryUsageAsync(string key, CancellationToken cancellationToken = default)
    {
        return OptionalInteger(new RespCommand("MEMORY", "USAGE", key), cancellationToken);
    }

    /// <summary>Sends <c>SLOWLOG LEN</c>.</summary>
    public ValueTask<long> GetSlowLogLengthAsync(CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("SLOWLOG", "LEN"), cancellationToken);
    }

    /// <summary>Sends <c>SLOWLOG RESET</c>.</summary>
    public ValueTask ResetSlowLogAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("SLOWLOG", "RESET"), cancellationToken);
    }

    /// <summary>Sends <c>SLOWLOG GET</c> for up to <paramref name="count"/> entries. Each entry is returned as the raw reply.</summary>
    public async ValueTask<IReadOnlyList<RespValue>> GetSlowLogAsync(long count = 10, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("SLOWLOG", "GET", count), cancellationToken).ConfigureAwait(false);

        return reply.Items ?? throw new RuvioException($"SLOWLOG returned {reply.Kind}");
    }

    /// <summary>Sends <c>CLIENT ID</c>.</summary>
    public ValueTask<long> GetClientIdAsync(CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("CLIENT", "ID"), cancellationToken);
    }

    /// <summary>Sends <c>CLIENT INFO</c>: one line for this connection.</summary>
    public async ValueTask<string> GetClientInfoAsync(CancellationToken cancellationToken = default)
    {
        return (await ExecuteCommandAsync(new RespCommand("CLIENT", "INFO"), cancellationToken).ConfigureAwait(false)).AsString()
            ?? throw new RuvioException("CLIENT INFO returned a null reply");
    }

    /// <summary>Sends <c>CLIENT LIST</c>: one line per connection on the answering shard.</summary>
    public async ValueTask<string> GetClientListAsync(CancellationToken cancellationToken = default)
    {
        return (await ExecuteCommandAsync(new RespCommand("CLIENT", "LIST"), cancellationToken).ConfigureAwait(false)).AsString()
            ?? throw new RuvioException("CLIENT LIST returned a null reply");
    }

    /// <summary>Sends <c>CLIENT KILL ID</c> and returns how many connections were closed.</summary>
    public ValueTask<long> KillClientAsync(long id, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("CLIENT", "KILL", "ID", id), cancellationToken);
    }

    /// <summary>Sends <c>LINSERT</c>. Returns the new length, -1 when the pivot is missing, 0 for a missing list.</summary>
    public ValueTask<long> InsertListAsync(string key, bool before, string pivot, string value, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("LINSERT", key, before ? "BEFORE" : "AFTER", pivot, value), cancellationToken);
    }

    /// <summary>Sends <c>LSET</c>.</summary>
    public ValueTask SetListItemAsync(string key, long index, string value, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("LSET", key, index, value), cancellationToken);
    }

    /// <summary>Sends <c>LREM</c> and returns how many elements were removed.</summary>
    public ValueTask<long> RemoveListItemsAsync(string key, long count, string value, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("LREM", key, count, value), cancellationToken);
    }

    /// <summary>Sends <c>LPOS</c> for the first match. Null when the value is not in the list.</summary>
    public ValueTask<long?> GetListPositionAsync(string key, string value, CancellationToken cancellationToken = default)
    {
        return OptionalInteger(new RespCommand("LPOS", key, value), cancellationToken);
    }

    /// <summary>Sends <c>LPUSHX</c>. Returns 0 and writes nothing when the list does not exist.</summary>
    public ValueTask<long> PushListLeftIfExistsAsync(string key, params string[] values)
    {
        return Integer(RespCommand.Keyed("LPUSHX", key, values));
    }

    /// <summary>Sends <c>RPUSHX</c>. Returns 0 and writes nothing when the list does not exist.</summary>
    public ValueTask<long> PushListRightIfExistsAsync(string key, params string[] values)
    {
        return Integer(RespCommand.Keyed("RPUSHX", key, values));
    }

    /// <summary>Sends <c>LMOVE</c>. Null when the source is empty.</summary>
    public ValueTask<string?> MoveListItemAsync(
        string source,
        string destination,
        bool fromLeft,
        bool toLeft,
        CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("LMOVE", source, destination, Side(fromLeft), Side(toLeft)), cancellationToken);
    }

    /// <summary>Sends <c>RPOPLPUSH</c>. Null when the source is empty.</summary>
    public ValueTask<string?> PopListRightPushLeftAsync(string source, string destination, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("RPOPLPUSH", source, destination), cancellationToken);
    }

    /// <summary>Sends <c>BLMOVE</c>. Null when <paramref name="timeout"/> passes; <see cref="TimeSpan.Zero"/> waits forever.</summary>
    public ValueTask<string?> MoveListItemBlockingAsync(
        string source,
        string destination,
        bool fromLeft,
        bool toLeft,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return BulkOrNull(
            new RespCommand("BLMOVE", source, destination, Side(fromLeft), Side(toLeft), Number((long)Math.Ceiling(timeout.TotalSeconds))),
            cancellationToken);
    }

    /// <summary>Sends <c>LMPOP</c>. Returns the key that was popped and its elements, or null when every list is empty.</summary>
    public async ValueTask<KeyValuePair<string, IReadOnlyList<string>>?> PopListManyAsync(
        IReadOnlyList<string> keys,
        bool fromLeft,
        long count = 1,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);

        var arguments = new List<string> { "LMPOP", Number(keys.Count) };

        arguments.AddRange(keys);
        arguments.Add(Side(fromLeft));
        arguments.Add("COUNT");
        arguments.Add(Number(count));
        RespValue reply = await ExecuteAsync(arguments, cancellationToken).ConfigureAwait(false);

        if (reply.Kind is RespKind.Null)
        {
            return null;
        }

        if (reply.Items is not { Count: 2 } items || items[1].Items is not { } elements)
        {
            throw new RuvioException($"LMPOP returned {reply.Kind}");
        }

        var values = new List<string>(elements.Count);

        foreach (RespValue element in elements)
        {
            values.Add(element.AsString() ?? "");
        }

        return new KeyValuePair<string, IReadOnlyList<string>>(items[0].AsString() ?? "", values);
    }

    /// <summary>
    /// Sends <c>BITFIELD</c>. <paramref name="operations"/> are the subcommand words, for example
    /// <c>"OVERFLOW", "SAT", "INCRBY", "u8", "0", "10", "GET", "u8", "0"</c>. Each result is a number, or null when
    /// <c>OVERFLOW FAIL</c> refused the write.
    /// </summary>
    public ValueTask<IReadOnlyList<long?>> BitFieldAsync(string key, params string[] operations)
    {
        return BitField("BITFIELD", key, operations);
    }

    /// <summary>Sends <c>BITFIELD_RO</c> with <c>GET type offset</c> subcommands.</summary>
    public ValueTask<IReadOnlyList<long?>> BitFieldReadOnlyAsync(string key, params string[] operations)
    {
        return BitField("BITFIELD_RO", key, operations);
    }

    private async ValueTask<IReadOnlyList<long?>> BitField(string name, string key, string[] operations)
    {
        ArgumentNullException.ThrowIfNull(operations);

        var arguments = new List<string> { name, key };

        arguments.AddRange(operations);
        RespValue reply = await ExecuteAsync(arguments, CancellationToken.None).ConfigureAwait(false);

        if (reply.Items is not { } items)
        {
            throw new RuvioException($"{name} returned {reply.Kind}");
        }

        var results = new long?[items.Count];

        for (int i = 0; i < results.Length; i++)
        {
            results[i] = items[i].Kind switch
            {
                RespKind.Null => null,
                RespKind.Integer => items[i].Integer,
                _ => throw new RuvioException($"{name} returned {items[i].Kind} in an array"),
            };
        }

        return results;
    }

    /// <summary>Sends <c>DUMP</c>. The payload is opaque and readable only by <c>RESTORE</c> on a Ruvio server. Null for a missing key.</summary>
    public async ValueTask<byte[]?> DumpAsync(string key, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteBinaryAsync([Bytes("DUMP"), Bytes(key)], cancellationToken).ConfigureAwait(false);

        return reply.Kind switch
        {
            RespKind.Null => null,
            RespKind.Bulk => reply.Bytes ?? [],
            _ => throw new RuvioException($"DUMP returned {reply.Kind}"),
        };
    }

    /// <summary>
    /// Sends <c>RESTORE</c>. <paramref name="ttlMilliseconds"/> 0 means no expiry; with <paramref name="absoluteTtl"/> it is a unix
    /// time in milliseconds. A key that exists is an error unless <paramref name="replace"/> is true.
    /// </summary>
    public async ValueTask RestoreAsync(
        string key,
        byte[] payload,
        long ttlMilliseconds = 0,
        bool replace = false,
        bool absoluteTtl = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(payload);

        var arguments = new List<ReadOnlyMemory<byte>> { Bytes("RESTORE"), Bytes(key), Bytes(Number(ttlMilliseconds)), payload };

        if (replace)
        {
            arguments.Add(Bytes("REPLACE"));
        }

        if (absoluteTtl)
        {
            arguments.Add(Bytes("ABSTTL"));
        }

        _ = await ExecuteBinaryAsync(arguments, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Sends <c>FILLGET</c>. One caller gets <see cref="FillState.Fill"/> and builds the value; the rest get
    /// <see cref="FillState.Hit"/>, <see cref="FillState.Busy"/> or <see cref="FillState.Stale"/>. <paramref name="fill"/> is how
    /// long the fill marker lives. With <paramref name="wait"/> the server holds the other callers until the filler writes
    /// or the wait passes.
    /// <para>
    /// <b>Warning:</b> the server parks the whole connection while it waits, so on a shared (multiplexed) client every
    /// other command queued behind it, including the filler's own <c>FILLSET</c>, stalls until the wait ends. Leave
    /// <paramref name="wait"/> unset on a shared client and poll instead (see <c>RuvioDistributedCache.GetOrCreateAsync</c>),
    /// or use it only on a dedicated connection.
    /// </para>
    /// </summary>
    /// <remarks>
    /// A <paramref name="wait"/> holds this client's whole connection, not only this call: every command sent on it
    /// afterwards, including the filler's <c>FILLSET</c> when it shares the client, is processed only after the wait ends.
    /// Use <paramref name="wait"/> on a connection of its own, or leave it out and poll with short pauses.
    /// </remarks>
    public async ValueTask<FillResult> FillGetAsync(
        string key,
        TimeSpan fill,
        TimeSpan? wait = null,
        CancellationToken cancellationToken = default)
    {
        RespCommand command = wait is { } limit
            ? new RespCommand("FILLGET", key, Milliseconds(fill), "WAIT", Milliseconds(limit))
            : new RespCommand("FILLGET", key, Milliseconds(fill));

        return FillResult.FromReply(await ExecuteCommandAsync(command, cancellationToken).ConfigureAwait(false));
    }

    /// <summary>
    /// Sends <c>FILLSET</c>. True when <paramref name="token"/> was valid and the value was written. <paramref name="stale"/> keeps
    /// the old value readable for that long after <paramref name="ttl"/> (stale-while-revalidate).
    /// </summary>
    public ValueTask<bool> FillSetAsync(
        string key,
        string value,
        TimeSpan ttl,
        long token,
        TimeSpan? stale = null,
        CancellationToken cancellationToken = default)
    {
        return stale is { } window
            ? Flag(new RespCommand(["FILLSET", key, value, Number(Milliseconds(ttl)), Number(token), "SWR", Number(Milliseconds(window))]), cancellationToken)
            : Flag(new RespCommand("FILLSET", key, value, Milliseconds(ttl), token), cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<long?> OptionalInteger(RespCommand arguments, CancellationToken cancellationToken)
    {
        RespValue reply = await ExecuteCommandAsync(arguments, cancellationToken).ConfigureAwait(false);

        return reply.Kind switch
        {
            RespKind.Null => null,
            RespKind.Integer => reply.Integer,
            _ => throw new RuvioException($"{arguments.Name} returned {reply.Kind}"),
        };
    }

    private static string Side(bool left) => left ? "LEFT" : "RIGHT";

    private static long Milliseconds(TimeSpan span) => Math.Max(1, (long)Math.Ceiling(span.TotalMilliseconds));

    private static ReadOnlyMemory<byte> Bytes(string text) => Encoding.UTF8.GetBytes(text);
}
