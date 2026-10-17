// client_coverage: APPEND DEL EVAL EXEC EXPIRE GET HGET HGETALL HSET INCR
// LPUSH LRANGE MGET MSET MULTI PING SADD SET SMEMBERS STRLEN TTL ZADD ZRANGE

using System.Globalization;
using System.Runtime.CompilerServices;

namespace Ruvio.Client;

public sealed partial class RuvioClient
{
    /// <summary>Begins a native idempotency operation. Returns [acquired], [pending], [conflict], or [completed, binary result].</summary>
    public ValueTask<RespValue> BeginIdempotencyAsync(string key, string fingerprint, string owner, long ttlMs,
        CancellationToken cancellationToken = default)
        => ExecuteCommandAsync(new RespCommand("IDEM", key, "BEGIN", fingerprint, owner, ttlMs), cancellationToken);

    /// <summary>Completes only the matching live owner; identical completion is acknowledged without rewriting or renewing TTL.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> CompleteIdempotencyAsync(string key, string fingerprint, string owner, string result,
        CancellationToken cancellationToken = default)
    {
        var reply = await ExecuteCommandAsync(new RespCommand("IDEM", key, "COMPLETE", fingerprint, owner, result), cancellationToken)
            .ConfigureAwait(false);

        if (reply.Kind != RespKind.Integer || reply.Integer is < 0 or > 1)
            throw new RuvioException("Invalid idempotency completion response.");

        return reply.Integer == 1;
    }

    /// <summary>Sends <c>LIMIT</c>. Null means this call did not fit. Otherwise the remaining allowance.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long?> LimitAsync(
        string key,
        long max,
        long windowMs,
        CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(
            new RespCommand("LIMIT", key, max, windowMs),
            cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer;
    }

    /// <summary>Sends <c>ONCE</c>. 1 is the first call. 0 means the key is already taken.</summary>
    public ValueTask<long> OnceAsync(string key, long ttlMs, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("ONCE", key, ttlMs), cancellationToken);
    }

    /// <summary>Sends <c>TAKE</c>. Null means the key did not hold enough.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long?> TakeAsync(string key, long count, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("TAKE", key, count), cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer;
    }

    /// <summary>Sends <c>GETEX</c>. Returns the string and sets its TTL.</summary>
    public ValueTask<string?> GetExpireAsync(string key, long ttlMs, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("GETEX", key, ttlMs), cancellationToken);
    }

    /// <summary>Sends <c>LEASE</c>. Null means the lease is still held.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long?> LeaseAsync(string key, long ttlMs, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("LEASE", key, ttlMs), cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer;
    }

    /// <summary>Sends <c>SEMAPHORE key max ttlMs</c>. A token while fewer than <paramref name="max"/> holders are live; null when full. Pass the token to <see cref="ReleaseAsync"/>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long?> AcquireSemaphoreAsync(string key, long max, long ttlMs, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("SEMAPHORE", key, max, ttlMs), cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer;
    }

    /// <summary>Sends <c>INCRBY key delta MAX limit</c>. Null means the result would pass <paramref name="limit"/>, and the key was left unchanged.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long?> IncrementWithLimitAsync(string key, long delta, long limit, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("INCRBY", key, delta, "MAX", limit), cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer;
    }

    /// <summary>Sends <c>RELEASE</c> for a lease or a semaphore token.</summary>
    public ValueTask<long> ReleaseAsync(string key, long token, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("RELEASE", key, token), cancellationToken);
    }

    /// <summary>Sends <c>SETV</c> and creates version 1.</summary>
    public ValueTask<long> SetVersionAsync(string key, string value, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("SETV", key, value), cancellationToken);
    }

    /// <summary>Sends <c>SETV</c> when <paramref name="version"/> is still current.</summary>
    public ValueTask<long> SetVersionAsync(string key, string value, long version, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("SETV", key, value, version), cancellationToken);
    }

    /// <summary>Sends <c>CHANGES START</c> and returns the cursor.</summary>
    public ValueTask<long> StartChangesAsync(CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("CHANGES", "START"), cancellationToken);
    }

    /// <summary>Sends <c>CHANGES</c> and returns notes after <paramref name="cursor"/>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<ChangeNote>> ReadChangesAsync(
        long cursor,
        CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("CHANGES", cursor), cancellationToken);

        if (reply.Items is null)
        {
            throw new RuvioException("CHANGES did not return an array");
        }

        var notes = new List<ChangeNote>(reply.Items.Count);

        foreach (RespValue row in reply.Items)
        {
            if (row.Items is not { Count: >= 2 } parts)
            {
                throw new RuvioException("CHANGES note is not a sequence and a text");
            }

            notes.Add(new ChangeNote(parts[0].Integer, parts[1].AsString() ?? ""));
        }

        return notes;
    }

    /// <summary>Sends <c>MGET</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<string?>> GetManyAsync(
        params string[] keys)
    {
        return await ArrayOfNullableBulks(Join("MGET", keys));
    }

    /// <summary>Sends <c>MSET</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SetManyAsync(params (string Key, string Value)[] pairs)
    {
        await Ok(FlattenPairs("MSET", pairs));
    }

    /// <summary>Sends <c>GETSET</c>.</summary>
    public ValueTask<string?> GetSetAsync(string key, string value, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("GETSET", key, value), cancellationToken);
    }

    /// <summary>Sends <c>JSON.GET</c>. Returns compact JSON, or null when the key or path is absent.</summary>
    public ValueTask<string?> JsonGetAsync(string key, string path = "$", CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("JSON.GET", key, path), cancellationToken);
    }

    /// <summary>Sends <c>JSON.MGET</c>: one path from several keys of one slot. A missing key is null.</summary>
    public async ValueTask<IReadOnlyList<string?>> JsonGetManyAsync(string path, params string[] keys)
    {
        string[] arguments = new string[keys.Length + 1];

        keys.CopyTo(arguments, 0);
        arguments[keys.Length] = path;

        return await ArrayOfNullableBulks(Join("JSON.MGET", arguments));
    }

    /// <summary>Sends <c>JSON.SET</c>. False when the path cannot be reached (nothing was written).</summary>
    public async ValueTask<bool> JsonSetAsync(string key, string path, string json, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("JSON.SET", key, path, json), cancellationToken);

        return reply.Kind is not RespKind.Null;
    }

    /// <summary>Sends <c>JSON.MERGE</c> (RFC 7396). False when the path cannot be reached.</summary>
    public async ValueTask<bool> JsonMergeAsync(string key, string path, string patch, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("JSON.MERGE", key, path, patch), cancellationToken);

        return reply.Kind is not RespKind.Null;
    }

    /// <summary>Sends <c>JSON.TYPE</c>: object, array, string, integer, number, boolean or null; null when absent.</summary>
    public ValueTask<string?> JsonTypeAsync(string key, string path = "$", CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("JSON.TYPE", key, path), cancellationToken);
    }

    /// <summary>Sends <c>JSON.STRLEN</c>. Null when the key or path is absent.</summary>
    public async ValueTask<long?> JsonStringLengthAsync(string key, string path = "$", CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("JSON.STRLEN", key, path), cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer;
    }

    /// <summary>Sends <c>JSON.ARRLEN</c>. Null when the key or path is absent.</summary>
    public async ValueTask<long?> JsonArrayLengthAsync(string key, string path = "$", CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("JSON.ARRLEN", key, path), cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer;
    }

    /// <summary>Sends <c>JSON.OBJKEYS</c>. Empty when the key or path is absent.</summary>
    public async ValueTask<IReadOnlyList<string>> JsonObjectKeysAsync(string key, string path = "$", CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("JSON.OBJKEYS", key, path), cancellationToken);

        if (reply.Items is null)
        {
            return [];
        }

        return reply.Items.Select(item => item.AsString() ?? string.Empty).ToArray();
    }

    /// <summary>Sends <c>JSON.NUMINCRBY</c> and returns the new number as JSON text; null when absent.</summary>
    public ValueTask<string?> JsonIncrementAsync(string key, string path, double by, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(
            new RespCommand("JSON.NUMINCRBY", key, path, by.ToString("R", CultureInfo.InvariantCulture)),
            cancellationToken);
    }

    /// <summary>Sends <c>JSON.TOGGLE</c> and returns the new boolean; null when absent.</summary>
    public async ValueTask<bool?> JsonToggleAsync(string key, string path = "$", CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("JSON.TOGGLE", key, path), cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer != 0;
    }

    /// <summary>Sends <c>JSON.ARRAPPEND</c> with one JSON value and returns the new length; null when absent.</summary>
    public async ValueTask<long?> JsonArrayAppendAsync(string key, string path, string json, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("JSON.ARRAPPEND", key, path, json), cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer;
    }

    /// <summary>Sends <c>JSON.CLEAR</c>: empties an array or object, zeroes a number. Returns how many values changed.</summary>
    public async ValueTask<long> JsonClearAsync(string key, string path = "$", CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("JSON.CLEAR", key, path), cancellationToken);

        return reply.Kind == RespKind.Null ? 0 : reply.Integer;
    }

    /// <summary>Sends <c>JSON.DEL</c>. Returns 1 when a field, element or the whole key was removed, else 0.</summary>
    public ValueTask<long> JsonDeleteAsync(string key, string path = "$", CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("JSON.DEL", key, path), cancellationToken);
    }

    /// <summary>Sends <c>APPEND</c>.</summary>
    public ValueTask<long> AppendAsync(string key, string value, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("APPEND", key, value), cancellationToken);
    }

    /// <summary>Sends <c>STRLEN</c>.</summary>
    public ValueTask<long> GetStringLengthAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("STRLEN", key), cancellationToken);
    }

    /// <summary>Sends <c>DECR</c>.</summary>
    public ValueTask<long> DecrementAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("DECR", key), cancellationToken);
    }

    /// <summary>Sends <c>INCRBY</c>.</summary>
    public ValueTask<long> IncrementByAsync(string key, long delta, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("INCRBY", key, delta), cancellationToken);
    }

    /// <summary>Sends <c>DECRBY</c>.</summary>
    public ValueTask<long> DecrementByAsync(string key, long delta, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("DECRBY", key, delta), cancellationToken);
    }

    /// <summary>Sends <c>UNLINK</c>.</summary>
    public ValueTask<long> UnlinkAsync(params string[] keys)
    {
        return Integer(Join("UNLINK", keys));
    }

    /// <summary>Sends <c>EXISTS</c>.</summary>
    public ValueTask<long> ExistsAsync(params string[] keys)
    {
        return Integer(Join("EXISTS", keys));
    }

    /// <summary>Sends <c>TYPE</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> GetKeyTypeAsync(string key, CancellationToken cancellationToken = default)
    {
        return (await ExecuteCommandAsync(new RespCommand("TYPE", key), cancellationToken)).AsString()
            ?? throw new RuvioException("TYPE returned a null reply");
    }

    /// <summary>Sends <c>RENAME</c>.</summary>
    public ValueTask RenameAsync(string key, string newKey, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("RENAME", key, newKey), cancellationToken);
    }

    /// <summary>Sends <c>SCAN</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<ScanPage> ScanAsync(
        long cursor,
        string? pattern = null,
        int? count = null,
        CancellationToken cancellationToken = default)
    {
        var arguments = new List<string> { "SCAN", Number(cursor) };

        if (pattern is not null)
        {
            arguments.Add("MATCH");
            arguments.Add(pattern);
        }

        if (count is not null)
        {
            arguments.Add("COUNT");
            arguments.Add(Number(count.Value));
        }

        return ParseScan(await ExecuteAsync(arguments, cancellationToken));
    }

    /// <summary>Sends <c>DBSIZE</c>.</summary>
    public ValueTask<long> GetDatabaseSizeAsync(CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("DBSIZE"), cancellationToken);
    }

    /// <summary>Sends <c>EXPIREAT</c> or <c>PEXPIREAT</c>.</summary>
    public ValueTask<bool> ExpireAtAsync(string key, DateTimeOffset when, CancellationToken cancellationToken = default)
    {
        long milliseconds = when.ToUnixTimeMilliseconds();

        return milliseconds % 1000 == 0
            ? Flag(new RespCommand("EXPIREAT", key, milliseconds / 1000), cancellationToken)
            : Flag(new RespCommand("PEXPIREAT", key, milliseconds), cancellationToken);
    }

    /// <summary>Sends <c>TTL</c>.</summary>
    public ValueTask<long> GetTimeToLiveAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("TTL", key), cancellationToken);
    }

    /// <summary>Sends <c>PTTL</c>.</summary>
    public ValueTask<long> GetTimeToLiveMillisecondsAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("PTTL", key), cancellationToken);
    }

    /// <summary>Sends <c>PERSIST</c>.</summary>
    public ValueTask<bool> PersistAsync(string key, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("PERSIST", key), cancellationToken);
    }

    /// <summary>Sends <c>LPUSH</c>.</summary>
    public ValueTask<long> PushListLeftAsync(string key, params string[] values)
    {
        return Integer(RespCommand.Keyed("LPUSH", key, values));
    }

    /// <summary>Sends <c>RPUSH</c>.</summary>
    public ValueTask<long> PushListRightAsync(string key, params string[] values)
    {
        return Integer(RespCommand.Keyed("RPUSH", key, values));
    }

    /// <summary>Sends <c>LPOP</c> for one element.</summary>
    public ValueTask<string?> PopListLeftAsync(string key, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("LPOP", key), cancellationToken);
    }

    /// <summary>Sends <c>LPOP</c> with a count. An empty list returns an empty array.</summary>
    public ValueTask<IReadOnlyList<string>> PopListLeftAsync(string key, long count, CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("LPOP", key, count), cancellationToken);
    }

    /// <summary>Sends <c>RPOP</c> for one element.</summary>
    public ValueTask<string?> PopListRightAsync(string key, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("RPOP", key), cancellationToken);
    }

    /// <summary>Sends <c>RPOP</c> with a count.</summary>
    public ValueTask<IReadOnlyList<string>> PopListRightAsync(string key, long count, CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("RPOP", key, count), cancellationToken);
    }

    /// <summary>Sends <c>LLEN</c>.</summary>
    public ValueTask<long> GetListLengthAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("LLEN", key), cancellationToken);
    }

    /// <summary>Sends <c>LINDEX</c>.</summary>
    public ValueTask<string?> GetListItemAsync(string key, long index, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("LINDEX", key, index), cancellationToken);
    }

    /// <summary>Sends <c>LRANGE</c>.</summary>
    public ValueTask<IReadOnlyList<string>> GetListRangeAsync(string key, long start, long stop, CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("LRANGE", key, start, stop), cancellationToken);
    }

    /// <summary>Sends <c>LTRIM</c>.</summary>
    public ValueTask TrimListAsync(string key, long start, long stop, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("LTRIM", key, start, stop), cancellationToken);
    }

    /// <summary>Sends <c>BLPOP</c> for one key.</summary>
    public ValueTask<KeyValuePair<string, string>?> PopListLeftBlockingAsync(
        string key,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return PopListLeftBlockingAsync([key], timeout, cancellationToken);
    }

    /// <summary>Sends <c>BLPOP</c>.</summary>
    public ValueTask<KeyValuePair<string, string>?> PopListLeftBlockingAsync(
        IReadOnlyList<string> keys,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return BlockPop("BLPOP", keys, timeout, cancellationToken);
    }

    /// <summary>Sends <c>BRPOP</c> for one key.</summary>
    public ValueTask<KeyValuePair<string, string>?> PopListRightBlockingAsync(
        string key,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return PopListRightBlockingAsync([key], timeout, cancellationToken);
    }

    /// <summary>Sends <c>BRPOP</c>.</summary>
    public ValueTask<KeyValuePair<string, string>?> PopListRightBlockingAsync(
        IReadOnlyList<string> keys,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return BlockPop("BRPOP", keys, timeout, cancellationToken);
    }

    /// <summary>Sends <c>SADD</c>.</summary>
    public ValueTask<long> AddSetMembersAsync(string key, params string[] members)
    {
        return Integer(RespCommand.Keyed("SADD", key, members));
    }

    /// <summary>Sends <c>SREM</c>.</summary>
    public ValueTask<long> RemoveSetMembersAsync(string key, params string[] members)
    {
        return Integer(RespCommand.Keyed("SREM", key, members));
    }

    /// <summary>Sends <c>SISMEMBER</c>.</summary>
    public ValueTask<bool> SetContainsAsync(string key, string member, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("SISMEMBER", key, member), cancellationToken);
    }

    /// <summary>Sends <c>SCARD</c>.</summary>
    public ValueTask<long> GetSetCountAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("SCARD", key), cancellationToken);
    }

    /// <summary>Sends <c>SMEMBERS</c>.</summary>
    public ValueTask<IReadOnlyList<string>> GetSetMembersAsync(string key, CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("SMEMBERS", key), cancellationToken);
    }

    /// <summary>Sends <c>SINTER</c>.</summary>
    public ValueTask<IReadOnlyList<string>> GetSetIntersectionAsync(params string[] keys)
    {
        return ArrayOfBulks(Join("SINTER", keys));
    }

    /// <summary>Sends <c>SUNION</c>.</summary>
    public ValueTask<IReadOnlyList<string>> GetSetUnionAsync(params string[] keys)
    {
        return ArrayOfBulks(Join("SUNION", keys));
    }

    /// <summary>Sends <c>SDIFF</c>.</summary>
    public ValueTask<IReadOnlyList<string>> GetSetDifferenceAsync(params string[] keys)
    {
        return ArrayOfBulks(Join("SDIFF", keys));
    }

    /// <summary>Sends <c>SINTERSTORE</c>.</summary>
    public ValueTask<long> StoreSetIntersectionAsync(string destination, params string[] keys)
    {
        return Integer(Join("SINTERSTORE", destination, keys));
    }

    /// <summary>Sends <c>SUNIONSTORE</c>.</summary>
    public ValueTask<long> StoreSetUnionAsync(string destination, params string[] keys)
    {
        return Integer(Join("SUNIONSTORE", destination, keys));
    }

    /// <summary>Sends <c>SDIFFSTORE</c>.</summary>
    public ValueTask<long> StoreSetDifferenceAsync(string destination, params string[] keys)
    {
        return Integer(Join("SDIFFSTORE", destination, keys));
    }

    /// <summary>Sends <c>SMOVE</c>.</summary>
    public ValueTask<bool> MoveSetMemberAsync(string source, string destination, string member, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("SMOVE", source, destination, member), cancellationToken);
    }

    /// <summary>Sends <c>SPOP</c>.</summary>
    public ValueTask<string?> PopSetMemberAsync(string key, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("SPOP", key), cancellationToken);
    }

    /// <summary>Sends <c>SPOP</c> with a count and returns the removed members.</summary>
    public ValueTask<IReadOnlyList<string>> PopSetMembersAsync(string key, long count, CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("SPOP", key, count), cancellationToken);
    }

    /// <summary>Sends <c>SRANDMEMBER</c>.</summary>
    public ValueTask<string?> GetSetRandomMemberAsync(string key, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("SRANDMEMBER", key), cancellationToken);
    }

    /// <summary>
    /// Sends <c>SRANDMEMBER</c> with a count. A positive count returns distinct members;
    /// a negative count may repeat members.
    /// </summary>
    public ValueTask<IReadOnlyList<string>> GetSetRandomMembersAsync(string key, long count, CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("SRANDMEMBER", key, count), cancellationToken);
    }

    /// <summary>Sends <c>HSET</c>.</summary>
    public ValueTask<long> SetHashFieldsAsync(string key, params (string Field, string Value)[] fields)
    {
        return Integer(RespCommand.KeyedPairs("HSET", key, fields));
    }

    /// <summary>Sends <c>HGET</c>.</summary>
    public ValueTask<string?> GetHashFieldAsync(string key, string field, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("HGET", key, field), cancellationToken);
    }

    /// <summary>Sends <c>HDEL</c>.</summary>
    public ValueTask<long> DeleteHashFieldsAsync(string key, params string[] fields)
    {
        return Integer(RespCommand.Keyed("HDEL", key, fields));
    }

    /// <summary>Sends <c>HLEN</c>.</summary>
    public ValueTask<long> GetHashLengthAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("HLEN", key), cancellationToken);
    }

    /// <summary>Sends <c>HGETALL</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyDictionary<string, string>> GetHashAllAsync(string key, CancellationToken cancellationToken = default)
    {
        return Pairs((await ExecuteCommandAsync(new RespCommand("HGETALL", key), cancellationToken)).Items);
    }

    /// <summary>Sends <c>HMGET</c>.</summary>
    public ValueTask<IReadOnlyList<string?>> GetHashFieldsAsync(string key, params string[] fields)
    {
        return ArrayOfNullableBulks(RespCommand.Keyed("HMGET", key, fields));
    }

    /// <summary>Sends <c>HEXISTS</c>.</summary>
    public ValueTask<bool> HashFieldExistsAsync(string key, string field, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("HEXISTS", key, field), cancellationToken);
    }

    /// <summary>Sends <c>HKEYS</c>.</summary>
    public ValueTask<IReadOnlyList<string>> GetHashFieldNamesAsync(string key, CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("HKEYS", key), cancellationToken);
    }

    /// <summary>Sends <c>HVALS</c>.</summary>
    public ValueTask<IReadOnlyList<string>> GetHashFieldValuesAsync(string key, CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("HVALS", key), cancellationToken);
    }

    /// <summary>Sends <c>HINCRBY</c>.</summary>
    public ValueTask<long> IncrementHashFieldAsync(string key, string field, long delta, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("HINCRBY", key, field, delta), cancellationToken);
    }

    /// <summary>
    /// Sets a TTL on hash fields: <c>HEXPIRE</c> for whole seconds, otherwise <c>HPEXPIRE</c>.
    /// One reply per field: 1 set, 0 condition not met, 2 deleted by a zero TTL, -2 no such field or key.
    /// </summary>
    public ValueTask<IReadOnlyList<long>> ExpireHashFieldsAsync(string key, TimeSpan ttl, params string[] fields)
    {
        return ExpireHashFieldsAsync(key, ttl, FieldExpireCondition.Always, fields);
    }

    /// <summary>Like <see cref="ExpireHashFieldsAsync(string, TimeSpan, string[])"/> with <c>NX</c>, <c>XX</c>, <c>GT</c>, or <c>LT</c>.</summary>
    public ValueTask<IReadOnlyList<long>> ExpireHashFieldsAsync(
        string key,
        TimeSpan ttl,
        FieldExpireCondition condition,
        params string[] fields)
    {
        long milliseconds = (long)Math.Ceiling(ttl.TotalMilliseconds);
        string[] head = milliseconds % 1000 == 0
            ? ["HEXPIRE", key, Number(milliseconds / 1000)]
            : ["HPEXPIRE", key, Number(milliseconds)];

        return IntegerArray(FieldBlock(head, condition, fields));
    }

    /// <summary>Sets an absolute deadline on hash fields: <c>HEXPIREAT</c> for a whole second, otherwise <c>HPEXPIREAT</c>. A past time deletes the field (reply 2).</summary>
    public ValueTask<IReadOnlyList<long>> ExpireHashFieldsAtAsync(string key, DateTimeOffset when, params string[] fields)
    {
        long milliseconds = when.ToUnixTimeMilliseconds();
        string[] head = milliseconds % 1000 == 0
            ? ["HEXPIREAT", key, Number(milliseconds / 1000)]
            : ["HPEXPIREAT", key, Number(milliseconds)];

        return IntegerArray(FieldBlock(head, FieldExpireCondition.Always, fields));
    }

    /// <summary>Sends <c>HTTL</c>: seconds left per field, -1 without a TTL, -2 when missing.</summary>
    public ValueTask<IReadOnlyList<long>> GetHashFieldTimeToLiveAsync(string key, params string[] fields)
    {
        return IntegerArray(FieldBlock(["HTTL", key], FieldExpireCondition.Always, fields));
    }

    /// <summary>Sends <c>HPTTL</c>: milliseconds left per field, -1 without a TTL, -2 when missing.</summary>
    public ValueTask<IReadOnlyList<long>> GetHashFieldTimeToLiveMillisecondsAsync(string key, params string[] fields)
    {
        return IntegerArray(FieldBlock(["HPTTL", key], FieldExpireCondition.Always, fields));
    }

    /// <summary>Sends <c>HEXPIRETIME</c>: the Unix second each field expires at.</summary>
    public ValueTask<IReadOnlyList<long>> GetHashFieldExpireTimeAsync(string key, params string[] fields)
    {
        return IntegerArray(FieldBlock(["HEXPIRETIME", key], FieldExpireCondition.Always, fields));
    }

    /// <summary>Sends <c>HPEXPIRETIME</c>: the Unix millisecond each field expires at.</summary>
    public ValueTask<IReadOnlyList<long>> GetHashFieldExpireTimeMillisecondsAsync(string key, params string[] fields)
    {
        return IntegerArray(FieldBlock(["HPEXPIRETIME", key], FieldExpireCondition.Always, fields));
    }

    /// <summary>Sends <c>HPERSIST</c>: 1 removed a TTL, -1 the field had none, -2 it is missing.</summary>
    public ValueTask<IReadOnlyList<long>> PersistHashFieldsAsync(string key, params string[] fields)
    {
        return IntegerArray(FieldBlock(["HPERSIST", key], FieldExpireCondition.Always, fields));
    }

    /// <summary>Sends <c>HSETNX</c>.</summary>
    public ValueTask<bool> SetHashFieldIfAbsentAsync(string key, string field, string value, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("HSETNX", key, field, value), cancellationToken);
    }

    /// <summary>Sends <c>HSTRLEN</c>.</summary>
    public ValueTask<long> GetHashFieldLengthAsync(string key, string field, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("HSTRLEN", key, field), cancellationToken);
    }

    /// <summary>Sends <c>HSCAN</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<HashScanPage> ScanHashAsync(string key, long cursor, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("HSCAN", key, cursor), cancellationToken);
        ScanPage page = ParseScan(reply);

        return new HashScanPage(page.Cursor, PairsFromFlat(page.Keys));
    }

    /// <summary>Sends <c>HSCAN</c> with <c>MATCH</c> and an optional <c>COUNT</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<HashScanPage> ScanHashAsync(
        string key,
        long cursor,
        string? pattern,
        int? count = null,
        CancellationToken cancellationToken = default)
    {
        var arguments = new List<string> { "HSCAN", key, Number(cursor) };

        if (pattern is not null)
        {
            arguments.Add("MATCH");
            arguments.Add(pattern);
        }

        if (count is not null)
        {
            arguments.Add("COUNT");
            arguments.Add(Number(count.Value));
        }

        ScanPage page = ParseScan(await ExecuteAsync(arguments, cancellationToken));

        return new HashScanPage(page.Cursor, PairsFromFlat(page.Keys));
    }

    /// <summary>Sends <c>ZADD</c>.</summary>
    public ValueTask<long> AddSortedSetAsync(string key, params SortedSetEntry[] entries)
    {
        var arguments = new List<string> { "ZADD", key };

        foreach (SortedSetEntry entry in entries)
        {
            arguments.Add(Number(entry.Score));
            arguments.Add(entry.Member);
        }

        return Integer(arguments);
    }

    /// <summary>Sends <c>ZADD</c> for one member. False when the member already existed.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> AddSortedSetAsync(
        string key,
        string member,
        double score,
        CancellationToken cancellationToken = default)
    {
        return await Integer(new RespCommand("ZADD", key, Number(score), member), cancellationToken) > 0;
    }

    /// <summary>Sends <c>ZADD</c> with <c>(member, score)</c> pairs: <c>AddSortedSetAsync("board", ("ada", 100), ("bob", 50))</c>.</summary>
    public ValueTask<long> AddSortedSetAsync(string key, params (string Member, double Score)[] entries)
    {
        return AddSortedSetAsync(key, SortedSetAddFlags.None, entries);
    }

    /// <summary>Sends <c>ZADD</c> with flags and <c>(member, score)</c> pairs.</summary>
    public ValueTask<long> AddSortedSetAsync(string key, SortedSetAddFlags flags, params (string Member, double Score)[] entries)
    {
        return AddSortedSetAsync(
            key,
            flags,
            Array.ConvertAll(entries, entry => new SortedSetEntry(entry.Member, entry.Score)));
    }

    /// <summary>
    /// Sends <c>ZADD</c> with <c>NX</c>, <c>XX</c>, <c>GT</c>, <c>LT</c>, or <c>CH</c>.
    /// Without <see cref="SortedSetAddFlags.Changed"/> the reply counts new members.
    /// </summary>
    public ValueTask<long> AddSortedSetAsync(string key, SortedSetAddFlags flags, params SortedSetEntry[] entries)
    {
        List<string> arguments = SortedSetAddArguments(key, flags);

        foreach (SortedSetEntry entry in entries)
        {
            arguments.Add(Number(entry.Score));
            arguments.Add(entry.Member);
        }

        return Integer(arguments);
    }

    /// <summary>
    /// Sends <c>ZADD ... INCR</c>. Returns the new score, or null when a flag skipped the update.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<double?> IncrementSortedSetAsync(
        string key,
        string member,
        double delta,
        SortedSetAddFlags flags,
        CancellationToken cancellationToken = default)
    {
        List<string> arguments = SortedSetAddArguments(key, flags);

        arguments.Add("INCR");
        arguments.Add(Number(delta));
        arguments.Add(member);

        string? text = await BulkOrNull(arguments, cancellationToken);

        return text is null ? null : ParseDouble(text);
    }

    private static List<string> SortedSetAddArguments(string key, SortedSetAddFlags flags)
    {
        var arguments = new List<string> { "ZADD", key };

        if (flags.HasFlag(SortedSetAddFlags.IfNotExists))
        {
            arguments.Add("NX");
        }

        if (flags.HasFlag(SortedSetAddFlags.IfExists))
        {
            arguments.Add("XX");
        }

        if (flags.HasFlag(SortedSetAddFlags.GreaterThan))
        {
            arguments.Add("GT");
        }

        if (flags.HasFlag(SortedSetAddFlags.LessThan))
        {
            arguments.Add("LT");
        }

        if (flags.HasFlag(SortedSetAddFlags.Changed))
        {
            arguments.Add("CH");
        }

        return arguments;
    }

    /// <summary>Sends <c>ZRANGE</c> or <c>ZREVRANGE</c> without scores.</summary>
    public ValueTask<IReadOnlyList<string>> GetSortedSetRangeByRankAsync(
        string key,
        long start,
        long stop,
        bool descending = false,
        CancellationToken cancellationToken = default)
    {
        string command = descending ? "ZREVRANGE" : "ZRANGE";

        return ArrayOfBulks(new RespCommand(command, key, start, stop), cancellationToken);
    }

    /// <summary>Sends <c>ZRANGE</c> or <c>ZREVRANGE</c> with <c>WITHSCORES</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetRangeByRankWithScoresAsync(
        string key,
        long start,
        long stop,
        bool descending = false,
        CancellationToken cancellationToken = default)
    {
        string command = descending ? "ZREVRANGE" : "ZRANGE";
        RespValue reply = await ExecuteCommandAsync(new RespCommand(command, key, start, stop, "WITHSCORES"), cancellationToken);

        return ParseSortedSet(reply);
    }

    /// <summary>Sends <c>ZRANGE</c> <c>BYLEX</c>. Bounds are <c>-</c>, <c>+</c>, <c>[member</c>, or <c>(member</c>.</summary>
    public ValueTask<IReadOnlyList<string>> GetSortedSetRangeByLexAsync(
        string key,
        string min,
        string max,
        CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("ZRANGE", key, min, max, "BYLEX"), cancellationToken);
    }

    /// <summary>Sends <c>ZRANGEBYSCORE</c> with <c>WITHSCORES</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetRangeByScoreWithScoresAsync(
        string key,
        string min,
        string max,
        CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("ZRANGEBYSCORE", key, min, max, "WITHSCORES"), cancellationToken);

        return ParseSortedSet(reply);
    }

    /// <summary>Sends <c>ZREM</c>.</summary>
    public ValueTask<long> RemoveSortedSetMembersAsync(string key, params string[] members)
    {
        return Integer(RespCommand.Keyed("ZREM", key, members));
    }

    /// <summary>Sends <c>ZPOPMIN key count</c>: removes and returns up to <paramref name="count"/> lowest-scored members.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<SortedSetEntry>> PopSortedSetMinAsync(string key, long count = 1, CancellationToken cancellationToken = default)
    {
        return ParseSortedSet(await ExecuteCommandAsync(new RespCommand("ZPOPMIN", key, count), cancellationToken));
    }

    /// <summary>Sends <c>ZPOPMAX key count</c>: removes and returns up to <paramref name="count"/> highest-scored members.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<SortedSetEntry>> PopSortedSetMaxAsync(string key, long count = 1, CancellationToken cancellationToken = default)
    {
        return ParseSortedSet(await ExecuteCommandAsync(new RespCommand("ZPOPMAX", key, count), cancellationToken));
    }

    /// <summary>Sends <c>BZPOPMIN</c> for one key. Null when the timeout passes.</summary>
    public ValueTask<(string Key, SortedSetEntry Entry)?> PopSortedSetMinBlockingAsync(
        string key,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return PopSortedSetMinBlockingAsync([key], timeout, cancellationToken);
    }

    /// <summary>Sends <c>BZPOPMIN</c>. The timeout rounds up to whole seconds; zero waits forever.</summary>
    public ValueTask<(string Key, SortedSetEntry Entry)?> PopSortedSetMinBlockingAsync(
        IReadOnlyList<string> keys,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return BlockPopSorted("BZPOPMIN", keys, timeout, cancellationToken);
    }

    /// <summary>Sends <c>BZPOPMAX</c> for one key. Null when the timeout passes.</summary>
    public ValueTask<(string Key, SortedSetEntry Entry)?> PopSortedSetMaxBlockingAsync(
        string key,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return PopSortedSetMaxBlockingAsync([key], timeout, cancellationToken);
    }

    /// <summary>Sends <c>BZPOPMAX</c>. The timeout rounds up to whole seconds; zero waits forever.</summary>
    public ValueTask<(string Key, SortedSetEntry Entry)?> PopSortedSetMaxBlockingAsync(
        IReadOnlyList<string> keys,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return BlockPopSorted("BZPOPMAX", keys, timeout, cancellationToken);
    }

    /// <summary>Sends <c>ZREMRANGEBYRANK</c>. <paramref name="stop"/> is inclusive; negative ranks count from the end.</summary>
    public ValueTask<long> RemoveSortedSetRangeByRankAsync(string key, long start, long stop, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("ZREMRANGEBYRANK", key, start, stop), cancellationToken);
    }

    /// <summary>Sends <c>ZREMRANGEBYSCORE</c>. Bounds accept <c>-inf</c>, <c>+inf</c>, and <c>(</c> for exclusive.</summary>
    public ValueTask<long> RemoveSortedSetRangeByScoreAsync(string key, string min, string max, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("ZREMRANGEBYSCORE", key, min, max), cancellationToken);
    }

    /// <summary>Sends <c>ZREMRANGEBYLEX</c>. Bounds are <c>-</c>, <c>+</c>, <c>[member</c>, or <c>(member</c>.</summary>
    public ValueTask<long> RemoveSortedSetRangeByLexAsync(string key, string min, string max, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("ZREMRANGEBYLEX", key, min, max), cancellationToken);
    }

    /// <summary>Sends <c>ZINCRBY</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<double> IncrementSortedSetAsync(string key, string member, double delta, CancellationToken cancellationToken = default)
    {
        string? text = await BulkOrNull(new RespCommand("ZINCRBY", key, Number(delta), member), cancellationToken);

        return ParseDouble(text ?? throw new RuvioException("ZINCRBY returned a null reply"));
    }

    /// <summary>Sends <c>ZCARD</c>.</summary>
    public ValueTask<long> GetSortedSetCountAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("ZCARD", key), cancellationToken);
    }

    /// <summary>Sends <c>ZSCORE</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<double?> GetSortedSetScoreAsync(string key, string member, CancellationToken cancellationToken = default)
    {
        string? text = await BulkOrNull(new RespCommand("ZSCORE", key, member), cancellationToken);

        return text is null ? null : ParseDouble(text);
    }

    /// <summary>Sends <c>GEOADD</c>. Each place is longitude, latitude, member.</summary>
    public ValueTask<long> AddGeoAsync(
        string key,
        params (double Longitude, double Latitude, string Member)[] places)
    {
        var arguments = new string[2 + places.Length * 3];

        arguments[0] = "GEOADD";
        arguments[1] = key;
        int cursor = 2;

        foreach ((double longitude, double latitude, string member) in places)
        {
            arguments[cursor++] = Number(longitude);
            arguments[cursor++] = Number(latitude);
            arguments[cursor++] = member;
        }

        return Integer(arguments, CancellationToken.None);
    }

    /// <summary>Sends <c>GEODIST</c>. Unit is <c>m</c>, <c>km</c>, <c>ft</c>, or <c>mi</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<double?> GeoDistanceAsync(
        string key,
        string from,
        string to,
        string unit = "m",
        CancellationToken cancellationToken = default)
    {
        string? text = await BulkOrNull(new RespCommand("GEODIST", key, from, to, unit), cancellationToken);

        return text is null ? null : ParseDouble(text);
    }

    /// <summary>Sends <c>GEODIST</c> with a typed unit, sent as a pre-encoded token. Example: <c>GeoDistanceAsync("cities", "a", "b", GeoUnit.Kilometers)</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<double?> GeoDistanceAsync(
        string key,
        string from,
        string to,
        GeoUnit unit,
        CancellationToken cancellationToken = default)
    {
        string? text = await BulkOrNull(new RespCommand("GEODIST", key, from, to, Keywords.Arg(unit)), cancellationToken);

        return text is null ? null : ParseDouble(text);
    }

    /// <summary>Sends <c>GEOHASH</c>. A missing member is null.</summary>
    public ValueTask<IReadOnlyList<string?>> GeoHashAsync(
        string key,
        params string[] members)
    {
        var arguments = new List<string> { "GEOHASH", key };

        arguments.AddRange(members);

        return ArrayOfNullableBulks(arguments, CancellationToken.None);
    }

    /// <summary>Sends <c>GEOPOS</c>. A missing member is null.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<(double Longitude, double Latitude)?>> GeoPositionAsync(
        string key,
        params string[] members)
    {
        var arguments = new List<string> { "GEOPOS", key };

        arguments.AddRange(members);
        RespValue reply = await ExecuteAsync(arguments, CancellationToken.None);

        if (reply.Kind is not RespKind.Array || reply.Items is null)
        {
            throw new RuvioException($"GEOPOS returned {reply.Kind}");
        }

        var points = new List<(double Longitude, double Latitude)?>(reply.Items.Count);

        foreach (RespValue item in reply.Items)
        {
            if (item.Kind is RespKind.Null)
            {
                points.Add(null);
                continue;
            }

            if (item.Kind is not RespKind.Array || item.Items is null || item.Items.Count != 2)
            {
                throw new RuvioException($"GEOPOS returned {item.Kind}");
            }

            points.Add((
                ParseDouble(item.Items[0].AsString() ?? throw new RuvioException("GEOPOS returned a null coordinate")),
                ParseDouble(item.Items[1].AsString() ?? throw new RuvioException("GEOPOS returned a null coordinate"))));
        }

        return points;
    }

    /// <summary>Sends <c>GEOSEARCH</c> from a longitude and latitude, ordered nearest first.</summary>
    public ValueTask<IReadOnlyList<string>> GeoSearchAsync(
        string key,
        double longitude,
        double latitude,
        double radius,
        string unit)
    {
        return ArrayOfBulks(
            new string[] { "GEOSEARCH", key, "FROMLONLAT", Number(longitude), Number(latitude), "BYRADIUS", Number(radius), unit, "ASC" },
            CancellationToken.None);
    }

    /// <summary>Sends <c>GEOSEARCHSTORE</c> from a member and a radius.</summary>
    public ValueTask<long> GeoSearchStoreAsync(
        string destination,
        string source,
        string member,
        double radius,
        string unit)
    {
        return Integer(
            new string[] { "GEOSEARCHSTORE", destination, source, "FROMMEMBER", member, "BYRADIUS", Number(radius), unit },
            CancellationToken.None);
    }

    /// <summary>Sends <c>GEORADIUSBYMEMBER</c>, ordered nearest first.</summary>
    public ValueTask<IReadOnlyList<string>> GeoRadiusByMemberAsync(
        string key,
        string member,
        double radius,
        string unit)
    {
        return ArrayOfBulks(
            new RespCommand("GEORADIUSBYMEMBER", key, member, Number(radius), unit, "ASC"),
            CancellationToken.None);
    }

    /// <summary>Sends <c>GEORADIUS</c>, ordered nearest first.</summary>
    public ValueTask<IReadOnlyList<string>> GeoRadiusAsync(
        string key,
        double longitude,
        double latitude,
        double radius,
        string unit)
    {
        return ArrayOfBulks(
            new string[] { "GEORADIUS", key, Number(longitude), Number(latitude), Number(radius), unit, "ASC" },
            CancellationToken.None);
    }

    /// <summary>Sends <c>GEOSEARCH</c> from a longitude and latitude with a typed unit and result order. Example: <c>GeoSearchAsync("cities", 32.8, 39.9, 100, GeoUnit.Kilometers)</c>.</summary>
    public ValueTask<IReadOnlyList<string>> GeoSearchAsync(
        string key,
        double longitude,
        double latitude,
        double radius,
        GeoUnit unit,
        GeoOrder order = GeoOrder.Ascending)
    {
        return ArrayOfBulks(
            new string[] { "GEOSEARCH", key, "FROMLONLAT", Number(longitude), Number(latitude), "BYRADIUS", Number(radius), Keywords.Word(unit), Keywords.Word(order) },
            CancellationToken.None);
    }

    /// <summary>Sends <c>GEOSEARCHSTORE</c> from a member and a radius with a typed unit.</summary>
    public ValueTask<long> GeoSearchStoreAsync(
        string destination,
        string source,
        string member,
        double radius,
        GeoUnit unit)
    {
        return Integer(
            new string[] { "GEOSEARCHSTORE", destination, source, "FROMMEMBER", member, "BYRADIUS", Number(radius), Keywords.Word(unit) },
            CancellationToken.None);
    }

    /// <summary>Sends <c>GEORADIUSBYMEMBER</c> with a typed unit and result order, both sent as pre-encoded tokens.</summary>
    public ValueTask<IReadOnlyList<string>> GeoRadiusByMemberAsync(
        string key,
        string member,
        double radius,
        GeoUnit unit,
        GeoOrder order = GeoOrder.Ascending)
    {
        return ArrayOfBulks(
            new RespCommand("GEORADIUSBYMEMBER", key, member, Number(radius), Keywords.Arg(unit), Keywords.Arg(order)),
            CancellationToken.None);
    }

    /// <summary>Sends <c>GEORADIUS</c> with a typed unit and result order.</summary>
    public ValueTask<IReadOnlyList<string>> GeoRadiusAsync(
        string key,
        double longitude,
        double latitude,
        double radius,
        GeoUnit unit,
        GeoOrder order = GeoOrder.Ascending)
    {
        return ArrayOfBulks(
            new string[] { "GEORADIUS", key, Number(longitude), Number(latitude), Number(radius), Keywords.Word(unit), Keywords.Word(order) },
            CancellationToken.None);
    }

    /// <summary>Sends <c>ZRANK</c> or <c>ZREVRANK</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long?> GetSortedSetRankAsync(
        string key,
        string member,
        bool descending = false,
        CancellationToken cancellationToken = default)
    {
        string command = descending ? "ZREVRANK" : "ZRANK";
        RespValue reply = await ExecuteCommandAsync(new RespCommand(command, key, member), cancellationToken);

        if (reply.Kind is RespKind.Null)
        {
            return null;
        }

        if (reply.Kind is not RespKind.Integer)
        {
            throw new RuvioException($"{command} returned {reply.Kind}");
        }

        return reply.Integer;
    }

    /// <summary>Sends <c>BF.RESERVE</c>.</summary>
    public ValueTask ReserveBloomFilterAsync(string key, double errorRate, long capacity, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("BF.RESERVE", key, Number(errorRate), capacity), cancellationToken);
    }

    /// <summary>Sends <c>BF.ADD</c>.</summary>
    public ValueTask<bool> AddBloomFilterAsync(string key, string item, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("BF.ADD", key, item), cancellationToken);
    }

    /// <summary>Sends <c>BF.EXISTS</c>.</summary>
    public ValueTask<bool> BloomFilterContainsAsync(string key, string item, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("BF.EXISTS", key, item), cancellationToken);
    }

    /// <summary>Sends <c>XADD NOMKSTREAM</c>. A missing key stays absent and the reply is null.</summary>
    public ValueTask<string?> AddStreamEntryIfExistsAsync(
        string key,
        string id,
        params (string Field, string Value)[] fields)
    {
        var arguments = new List<string> { "XADD", key, "NOMKSTREAM" };

        arguments.AddRange(FlattenPairs(id, fields));

        return BulkOrNull(arguments);
    }

    /// <summary>Sends <c>XADD</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> AddStreamEntryAsync(string key, string id, params (string Field, string Value)[] fields)
    {
        string? result = await BulkOrNull(FlattenPairs("XADD", key, id, fields));

        return result ?? throw new RuvioException("XADD returned a null reply");
    }

    /// <summary>Sends <c>XADD key DELAY ms</c>. <c>XREADGROUP</c> skips the entry until the delay passes.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> AddStreamEntryDelayedAsync(
        string key,
        long delayMs,
        string id,
        params (string Field, string Value)[] fields)
    {
        var arguments = new List<string> { "XADD", key, "DELAY", Number(delayMs) };

        arguments.AddRange(FlattenPairs(id, fields));
        string? result = await BulkOrNull(arguments);

        return result ?? throw new RuvioException("XADD returned a null reply");
    }

    /// <summary>Sends <c>XADD key MAXLEN n</c>: the stream keeps exactly the newest <paramref name="maxLength"/> entries.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> AddStreamEntryAsync(
        string key,
        string id,
        long maxLength,
        params (string Field, string Value)[] fields)
    {
        var arguments = new List<string> { "XADD", key, "MAXLEN", Number(maxLength) };

        arguments.AddRange(FlattenPairs(id, fields));
        string? result = await BulkOrNull(arguments);

        return result ?? throw new RuvioException("XADD returned a null reply");
    }

    /// <summary>Sends <c>XLEN</c>.</summary>
    public ValueTask<long> GetStreamLengthAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("XLEN", key), cancellationToken);
    }

    /// <summary>Sends <c>XINFO STREAM</c>.</summary>
    public ValueTask<RespValue> GetStreamInfoAsync(string key, CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("XINFO", "STREAM", key), cancellationToken);
    }

    /// <summary>Sends <c>XINFO GROUPS</c>.</summary>
    public ValueTask<RespValue> GetStreamGroupsAsync(string key, CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("XINFO", "GROUPS", key), cancellationToken);
    }

    /// <summary>Sends <c>XSETID</c>. <paramref name="id"/> is <c>ms-seq</c> and must not be below the stream's top id.</summary>
    public ValueTask SetStreamIdAsync(string key, string id, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("XSETID", key, id), cancellationToken);
    }

    /// <summary>Sends <c>XAUTOCLAIM</c>. <paramref name="start"/> is exclusive; <c>0-0</c> starts at the beginning.</summary>
    public ValueTask<RespValue> AutoClaimStreamAsync(
        string key,
        string group,
        string consumer,
        long minIdleMs,
        string start,
        CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(
            new RespCommand("XAUTOCLAIM", key, group, consumer, minIdleMs, start),
            cancellationToken);
    }

    /// <summary>Sends <c>XINFO CONSUMERS</c>.</summary>
    public ValueTask<RespValue> GetStreamConsumersAsync(
        string key,
        string group,
        CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("XINFO", "CONSUMERS", key, group), cancellationToken);
    }

    /// <summary>Sends <c>XRANGE</c> or <c>XREVRANGE</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<StreamEntry>> GetStreamRangeAsync(
        string key,
        string start,
        string end,
        bool reverse = false,
        CancellationToken cancellationToken = default)
    {
        string command = reverse ? "XREVRANGE" : "XRANGE";

        return ParseStreamEntries(await ExecuteCommandAsync(new RespCommand(command, key, start, end), cancellationToken));
    }

    /// <summary>Sends <c>XRANGE</c> or <c>XREVRANGE</c> with <c>COUNT</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<StreamEntry>> GetStreamRangeAsync(
        string key,
        string start,
        string end,
        int count,
        bool reverse = false,
        CancellationToken cancellationToken = default)
    {
        string command = reverse ? "XREVRANGE" : "XRANGE";
        RespValue reply = await ExecuteCommandAsync(new RespCommand(command, key, start, end, "COUNT", count), cancellationToken);

        return ParseStreamEntries(reply);
    }

    /// <summary>Sends <c>XDEL</c>.</summary>
    public ValueTask<long> DeleteStreamEntriesAsync(string key, params string[] ids)
    {
        return Integer(Join("XDEL", key, ids));
    }

    /// <summary>Sends <c>XTRIM MINID</c>. Entries below <paramref name="id"/> are removed.</summary>
    public ValueTask<long> TrimStreamMinIdAsync(string key, string id, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("XTRIM", key, "MINID", id), cancellationToken);
    }

    /// <summary>Sends <c>XTRIM MAXLEN</c>.</summary>
    public ValueTask<long> TrimStreamAsync(string key, long maxLength, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("XTRIM", key, "MAXLEN", maxLength), cancellationToken);
    }

    /// <summary>Sends <c>XREAD STREAMS</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<StreamReadResult>> GetStreamUpdatesAsync(
        params (string Key, string Id)[] streams)
    {
        var arguments = new List<string> { "XREAD", "STREAMS" };

        AddStreams(arguments, streams);

        return ParseStreamRead(await ExecuteAsync(arguments));
    }

    /// <summary>
    /// Sends <c>XREAD</c> with <c>COUNT</c> and <c>BLOCK</c>. A block that times out returns an empty list.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<StreamReadResult>> GetStreamUpdatesAsync(
        StreamReadOptions options,
        params (string Key, string Id)[] streams)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.NoAcknowledge)
        {
            throw new ArgumentException("NOACK applies only to XREADGROUP.", nameof(options));
        }

        var arguments = new List<string> { "XREAD" };

        AddStreamReadOptions(arguments, options);

        arguments.Add("STREAMS");
        AddStreams(arguments, streams);

        return ParseStreamRead(await ExecuteAsync(arguments));
    }

    /// <summary>Sends <c>XGROUP CREATECONSUMER</c>. Returns 1 when the consumer name is new.</summary>
    public ValueTask<long> CreateStreamConsumerAsync(
        string key,
        string group,
        string consumer,
        CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("XGROUP", "CREATECONSUMER", key, group, consumer), cancellationToken);
    }

    /// <summary>Sends <c>XGROUP CREATE</c>.</summary>
    public ValueTask CreateStreamGroupAsync(string key, string group, string id, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("XGROUP", "CREATE", key, group, id), cancellationToken);
    }

    /// <summary>Sends <c>XGROUP CREATE</c>; with <paramref name="createStream"/> it adds <c>MKSTREAM</c>.</summary>
    public ValueTask CreateStreamGroupAsync(
        string key,
        string group,
        string id,
        bool createStream,
        CancellationToken cancellationToken = default)
    {
        IReadOnlyList<string> arguments = createStream
            ? ["XGROUP", "CREATE", key, group, id, "MKSTREAM"]
            : ["XGROUP", "CREATE", key, group, id];

        return Ok(new RespCommand(arguments), cancellationToken);
    }

    /// <summary>Sends <c>XREADGROUP GROUP</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<StreamReadResult>> GetStreamGroupUpdatesAsync(
        string group,
        string consumer,
        string key,
        string id,
        CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteAsync(
            ["XREADGROUP", "GROUP", group, consumer, "STREAMS", key, id],
            cancellationToken);

        return ParseStreamRead(reply);
    }

    /// <summary>
    /// Sends <c>XREADGROUP GROUP</c> with <c>COUNT</c>, <c>BLOCK</c>, and <c>NOACK</c>.
    /// A block that times out returns an empty list.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<StreamReadResult>> GetStreamGroupUpdatesAsync(
        string group,
        string consumer,
        string key,
        string id,
        StreamReadOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);

        var arguments = new List<string> { "XREADGROUP", "GROUP", group, consumer };

        AddStreamReadOptions(arguments, options);

        if (options.NoAcknowledge)
        {
            arguments.Add("NOACK");
        }

        arguments.AddRange(["STREAMS", key, id]);

        return ParseStreamRead(await ExecuteAsync(arguments, cancellationToken));
    }

    private static void AddStreamReadOptions(List<string> arguments, StreamReadOptions options)
    {
        if (options.Count is { } count)
        {
            arguments.Add("COUNT");
            arguments.Add(Number(count));
        }

        if (options.Block is { } block)
        {
            if (block < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(options), "Block cannot be negative.");
            }

            arguments.Add("BLOCK");
            arguments.Add(Number((long)Math.Ceiling(block.TotalMilliseconds)));
        }
    }

    /// <summary>Sends <c>XGROUP DESTROY</c>.</summary>
    public ValueTask<long> DestroyStreamGroupAsync(string key, string group, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("XGROUP", "DESTROY", key, group), cancellationToken);
    }

    /// <summary>Sends <c>XGROUP SETID</c>.</summary>
    public ValueTask SetStreamGroupIdAsync(string key, string group, string id, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("XGROUP", "SETID", key, group, id), cancellationToken);
    }

    /// <summary>Sends <c>XGROUP DELCONSUMER</c>.</summary>
    public ValueTask<long> DeleteStreamGroupConsumerAsync(string key, string group, string consumer, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("XGROUP", "DELCONSUMER", key, group, consumer), cancellationToken);
    }

    /// <summary>Sends <c>XACK</c>.</summary>
    public ValueTask<long> AcknowledgeStreamAsync(string key, string group, params string[] ids)
    {
        return Integer(Join("XACK", key, group, ids));
    }

    /// <summary>Sends <c>XPENDING</c>.</summary>
    public ValueTask<RespValue> GetStreamPendingAsync(string key, string group, CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("XPENDING", key, group), cancellationToken);
    }

    /// <summary>
    /// Sends the extended <c>XPENDING</c> form: ids from <paramref name="start"/> to <paramref name="end"/>
    /// (inclusive), at most <paramref name="count"/>, optionally only one consumer and entries idle
    /// for at least <paramref name="minIdle"/>.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<StreamPendingEntry>> GetStreamPendingAsync(
        string key,
        string group,
        string start,
        string end,
        long count,
        string? consumer = null,
        TimeSpan? minIdle = null,
        CancellationToken cancellationToken = default)
    {
        var arguments = new List<string> { "XPENDING", key, group };

        if (minIdle is { } idle)
        {
            arguments.Add("IDLE");
            arguments.Add(Number((long)Math.Ceiling(idle.TotalMilliseconds)));
        }

        arguments.AddRange([start, end, Number(count)]);

        if (consumer is not null)
        {
            arguments.Add(consumer);
        }

        RespValue reply = await ExecuteAsync(arguments, cancellationToken);
        var rows = new List<StreamPendingEntry>();

        foreach (RespValue row in reply.Items ?? [])
        {
            if (row.Items is not { Count: 4 } fields)
            {
                throw new RuvioException("XPENDING returned an unexpected row");
            }

            rows.Add(new StreamPendingEntry(
                fields[0].AsString() ?? "",
                fields[1].AsString() ?? "",
                fields[2].Integer,
                fields[3].Integer));
        }

        return rows;
    }

    /// <summary>Sends <c>XCLAIM</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<StreamEntry>> ClaimStreamEntriesAsync(
        string key,
        string group,
        string consumer,
        long minIdleMilliseconds,
        params string[] ids)
    {
        var arguments = new List<string> { "XCLAIM", key, group, consumer, Number(minIdleMilliseconds) };

        arguments.AddRange(ids);

        return ParseStreamEntries(await ExecuteAsync(arguments));
    }

    /// <summary>Sends <c>XCLAIM</c> with <c>IDLE</c>, <c>TIME</c>, <c>RETRYCOUNT</c>, <c>FORCE</c>, or <c>LASTID</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<StreamEntry>> ClaimStreamEntriesAsync(
        string key,
        string group,
        string consumer,
        long minIdleMilliseconds,
        StreamClaimOptions options,
        params string[] ids)
    {
        List<string> arguments = ClaimArguments(key, group, consumer, minIdleMilliseconds, options, ids);

        return ParseStreamEntries(await ExecuteAsync(arguments));
    }

    /// <summary>Sends <c>XCLAIM ... JUSTID</c> and returns only the claimed ids.</summary>
    public ValueTask<IReadOnlyList<string>> ClaimStreamEntryIdsAsync(
        string key,
        string group,
        string consumer,
        long minIdleMilliseconds,
        StreamClaimOptions? options,
        params string[] ids)
    {
        List<string> arguments = ClaimArguments(key, group, consumer, minIdleMilliseconds, options, ids);

        arguments.Add("JUSTID");

        return ArrayOfBulks(arguments);
    }

    private static List<string> ClaimArguments(
        string key,
        string group,
        string consumer,
        long minIdleMilliseconds,
        StreamClaimOptions? options,
        string[] ids)
    {
        var arguments = new List<string> { "XCLAIM", key, group, consumer, Number(minIdleMilliseconds) };

        arguments.AddRange(ids);

        if (options is null)
        {
            return arguments;
        }

        if (options.Idle is not null && options.Time is not null)
        {
            throw new ArgumentException("IDLE and TIME cannot be combined.", nameof(options));
        }

        if (options.Idle is { } idle)
        {
            arguments.Add("IDLE");
            arguments.Add(Number((long)Math.Ceiling(idle.TotalMilliseconds)));
        }

        if (options.Time is { } time)
        {
            arguments.Add("TIME");
            arguments.Add(Number(time.ToUnixTimeMilliseconds()));
        }

        if (options.RetryCount is { } retryCount)
        {
            arguments.Add("RETRYCOUNT");
            arguments.Add(Number(retryCount));
        }

        if (options.Force)
        {
            arguments.Add("FORCE");
        }

        if (options.LastId is not null)
        {
            arguments.Add("LASTID");
            arguments.Add(options.LastId);
        }

        return arguments;
    }

    /// <summary>Sends <c>INFO</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> GetInfoAsync(CancellationToken cancellationToken = default)
    {
        return await BulkOrNull(new RespCommand("INFO"), cancellationToken)
            ?? throw new RuvioException("INFO returned a null reply");
    }

    /// <summary>Sends <c>INFO section</c>. <c>prometheus</c> returns Prometheus text.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> GetInfoAsync(string section, CancellationToken cancellationToken = default)
    {
        return await BulkOrNull(new RespCommand("INFO", section), cancellationToken)
            ?? throw new RuvioException("INFO returned a null reply");
    }

    /// <summary>Sends <c>SAVE</c>.</summary>
    public ValueTask SaveAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("SAVE"), cancellationToken);
    }

    /// <summary>Sends <c>BGSAVE</c>.</summary>
    public ValueTask SaveInBackgroundAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("BGSAVE"), cancellationToken);
    }

    /// <summary>Sends <c>PUBLISH</c>.</summary>
    public ValueTask<long> PublishAsync(string channel, string message, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("PUBLISH", channel, message), cancellationToken);
    }

    /// <summary>Sends <c>SPUBLISH</c>.</summary>
    public ValueTask<long> PublishShardedAsync(string channel, string message, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("SPUBLISH", channel, message), cancellationToken);
    }

    /// <summary>Sends <c>EVAL</c>.</summary>
    public ValueTask<RespValue> EvaluateAsync(string script, long keyCount, params string[] arguments)
    {
        return ExecuteAsync(Join("EVAL", script, Number(keyCount), arguments));
    }

    /// <summary>Sends <c>MULTI</c>.</summary>
    public ValueTask BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("MULTI"), cancellationToken);
    }

    /// <summary>Sends <c>EXEC</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<RespValue>> ExecuteTransactionAsync(CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("EXEC"), cancellationToken);

        return reply.Items ?? [];
    }

    /// <summary>Sends <c>DISCARD</c>.</summary>
    public ValueTask DiscardTransactionAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("DISCARD"), cancellationToken);
    }

    /// <summary>Sends <c>WATCH</c>.</summary>
    public ValueTask WatchAsync(params string[] keys)
    {
        return Ok(Join("WATCH", keys));
    }

    /// <summary>Sends <c>UNWATCH</c>.</summary>
    public ValueTask UnwatchAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("UNWATCH"), cancellationToken);
    }

    /// <summary>Sends <c>ACL WHOAMI</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> GetAclUserNameAsync(CancellationToken cancellationToken = default)
    {
        return await BulkOrNull(new RespCommand("ACL", "WHOAMI"), cancellationToken)
            ?? throw new RuvioException("ACL WHOAMI returned a null reply");
    }

    /// <summary>Sends <c>CLUSTER SLOTS</c>.</summary>
    public ValueTask<RespValue> GetClusterSlotsAsync(CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("CLUSTER", "SLOTS"), cancellationToken);
    }

    /// <summary>Sends <c>CLUSTER NODES</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> GetClusterNodesAsync(CancellationToken cancellationToken = default)
    {
        return await BulkOrNull(new RespCommand("CLUSTER", "NODES"), cancellationToken)
            ?? throw new RuvioException("CLUSTER NODES returned a null reply");
    }

    /// <summary>Sends <c>CLUSTER SHARDS</c>.</summary>
    public ValueTask<RespValue> GetClusterShardsAsync(CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("CLUSTER", "SHARDS"), cancellationToken);
    }

    /// <summary>Sends <c>CLUSTER INFO</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> GetClusterInfoAsync(CancellationToken cancellationToken = default)
    {
        return await BulkOrNull(new RespCommand("CLUSTER", "INFO"), cancellationToken)
            ?? throw new RuvioException("CLUSTER INFO returned a null reply");
    }

    /// <summary>Sends <c>CLUSTER MYID</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> GetClusterMyIdAsync(CancellationToken cancellationToken = default)
    {
        return await BulkOrNull(new RespCommand("CLUSTER", "MYID"), cancellationToken)
            ?? throw new RuvioException("CLUSTER MYID returned a null reply");
    }

    /// <summary>Sends <c>CLUSTER KEYSLOT</c>.</summary>
    public ValueTask<long> GetClusterKeySlotAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("CLUSTER", "KEYSLOT", key), cancellationToken);
    }

    /// <summary>Sends <c>READONLY</c>. Ruvio has no replicas; the reply is still <c>OK</c> in cluster mode.</summary>
    public ValueTask ReadOnlyAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("READONLY"), cancellationToken);
    }

    /// <summary>Sends <c>READWRITE</c>.</summary>
    public ValueTask ReadWriteAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("READWRITE"), cancellationToken);
    }

    /// <summary>Sends <c>SCRIPT LOAD</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> LoadScriptAsync(string script, CancellationToken cancellationToken = default)
    {
        return await BulkOrNull(new RespCommand("SCRIPT", "LOAD", script), cancellationToken)
            ?? throw new RuvioException("SCRIPT LOAD returned a null reply");
    }

    /// <summary>Sends <c>EVALSHA</c>.</summary>
    public ValueTask<RespValue> EvaluateShaAsync(string sha, long keyCount, params string[] arguments)
    {
        return ExecuteAsync(Join("EVALSHA", sha, Number(keyCount), arguments));
    }

    /// <summary>Sends <c>SCRIPT EXISTS</c>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<bool>> ScriptExistsAsync(params string[] hashes)
    {
        RespValue reply = await ExecuteAsync(Join("SCRIPT", "EXISTS", hashes));

        IReadOnlyList<RespValue> items = reply.Items ?? [];
        var exists = new bool[items.Count];

        for (int index = 0; index < exists.Length; index++)
        {
            exists[index] = items[index].Integer > 0;
        }

        return exists;
    }

    /// <summary>Sends <c>SCRIPT FLUSH</c>.</summary>
    public ValueTask FlushScriptsAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("SCRIPT", "FLUSH"), cancellationToken);
    }

    /// <summary>Sends <c>SCRIPT KILL</c>.</summary>
    public ValueTask KillScriptAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("SCRIPT", "KILL"), cancellationToken);
    }

    /// <summary>Sends <c>FCALL</c>.</summary>
    public ValueTask<RespValue> CallFunctionAsync(string name, long keyCount, params string[] arguments)
    {
        return ExecuteAsync(Join("FCALL", name, Number(keyCount), arguments));
    }

    /// <summary>
    /// Sends <c>FUNCTION LOAD name body</c>. The body is plain Lua, not a Redis library shebang,
    /// and is lost when the server restarts.
    /// </summary>
    public ValueTask LoadFunctionAsync(string name, string body, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("FUNCTION", "LOAD", name, body), cancellationToken);
    }

    /// <summary>Sends <c>FUNCTION LIST</c>.</summary>
    public ValueTask<RespValue> GetFunctionListAsync(CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("FUNCTION", "LIST"), cancellationToken);
    }

    /// <summary>Sends <c>FUNCTION DELETE</c>. False when no function had that name.</summary>
    public ValueTask<bool> DeleteFunctionAsync(string name, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("FUNCTION", "DELETE", name), cancellationToken);
    }

    /// <summary>Sends <c>ACL USERS</c>.</summary>
    public ValueTask<IReadOnlyList<string>> GetAclUsersAsync(CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("ACL", "USERS"), cancellationToken);
    }

    /// <summary>Sends <c>CLIENT GETNAME</c>.</summary>
    public ValueTask<string?> GetConnectionNameAsync(CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("CLIENT", "GETNAME"), cancellationToken);
    }

    /// <summary>Sends <c>CLIENT SETNAME</c>.</summary>
    public ValueTask SetConnectionNameAsync(string name, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("CLIENT", "SETNAME", name), cancellationToken);
    }

    /// <summary>Sends <c>CLIENT TRACKING ON|OFF</c>.</summary>
    public ValueTask SetTrackingAsync(bool enabled, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("CLIENT", "TRACKING", enabled ? "ON" : "OFF"), cancellationToken);
    }

    /// <summary>Sends <c>CONFIG GET</c>.</summary>
    public ValueTask<RespValue> GetConfigAsync(string parameter, CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("CONFIG", "GET", parameter), cancellationToken);
    }

    /// <summary>Sends <c>FLUSHDB</c>. Only the selected database is emptied.</summary>
    public ValueTask FlushDatabaseAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("FLUSHDB"), cancellationToken);
    }

    /// <summary>Sends <c>FLUSHALL</c>. Every database is emptied.</summary>
    public ValueTask FlushAllDatabasesAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("FLUSHALL"), cancellationToken);
    }

    /// <summary>Sends <c>SWAPDB</c>.</summary>
    public ValueTask SwapDatabasesAsync(int first, int second, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("SWAPDB", first, second), cancellationToken);
    }

    /// <summary>Sends <c>MOVE</c>. False when the key is missing or already exists in <paramref name="database"/>.</summary>
    public ValueTask<bool> MoveKeyAsync(string key, int database, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("MOVE", key, database), cancellationToken);
    }

    /// <summary>Sends <c>HELLO</c>. Connecting itself does not send this.</summary>
    public ValueTask<RespValue> HelloAsync(int protocol = 2, CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("HELLO", protocol), cancellationToken);
    }

    /// <summary>Sends <c>ACL LIST</c>.</summary>
    public ValueTask<IReadOnlyList<string>> GetAclListAsync(CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("ACL", "LIST"), cancellationToken);
    }

    /// <summary>Sends <c>ACL GETUSER</c>.</summary>
    public ValueTask<RespValue> GetAclUserAsync(string user, CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("ACL", "GETUSER", user), cancellationToken);
    }

    /// <summary>Sends <c>ACL CAT</c>.</summary>
    public ValueTask<IReadOnlyList<string>> GetAclCategoriesAsync(CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("ACL", "CAT"), cancellationToken);
    }

    /// <summary>Sends <c>ACL SETUSER</c>.</summary>
    public ValueTask SetAclUserAsync(string user, params string[] rules)
    {
        return Ok(Join("ACL", "SETUSER", user, rules));
    }

    /// <summary>Sends <c>ACL LOAD</c>.</summary>
    public ValueTask LoadAclAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("ACL", "LOAD"), cancellationToken);
    }

    /// <summary>Sends <c>ACL SAVE</c>.</summary>
    public ValueTask SaveAclAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("ACL", "SAVE"), cancellationToken);
    }

    /// <summary>
    /// Sends <c>SUBSCRIBE</c> on a dedicated socket (not the command socket) and
    /// reads one confirmation per channel.
    /// </summary>
    public ValueTask<IReadOnlyList<RespValue>> SubscribeAsync(params string[] channels)
    {
        return ExecuteManyReplies(Join("SUBSCRIBE", channels), channels.Length);
    }

    /// <summary>
    /// Subscribes on a dedicated socket and invokes <paramref name="handler"/> for
    /// later <c>message</c> deliveries on <paramref name="channel"/>. Same idea as
    /// StackExchange.Redis <c>ISubscriber.Subscribe</c>.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SubscribeAsync(
        string channel,
        Action<PubSubMessage> handler,
        CancellationToken cancellationToken = default)
    {
        RegisterHandler(channel, handler);

        await ExecuteManyReplies(Join("SUBSCRIBE", channel), 1, cancellationToken);
    }

    /// <summary>Same as the action overload, with an async handler.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SubscribeAsync(
        string channel,
        Func<PubSubMessage, Task> handler,
        CancellationToken cancellationToken = default)
    {
        RegisterHandler(channel, handler);

        await ExecuteManyReplies(Join("SUBSCRIBE", channel), 1, cancellationToken);
    }

    /// <summary>
    /// Subscribes to a pattern on a dedicated socket and invokes
    /// <paramref name="handler"/> for matching <c>pmessage</c> deliveries.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SubscribePatternsAsync(
        string pattern,
        Action<PubSubMessage> handler,
        CancellationToken cancellationToken = default)
    {
        RegisterHandler(pattern, handler);

        await ExecuteManyReplies(Join("PSUBSCRIBE", pattern), 1, cancellationToken);
    }

    /// <summary>Same as the action overload, with an async handler.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SubscribePatternsAsync(
        string pattern,
        Func<PubSubMessage, Task> handler,
        CancellationToken cancellationToken = default)
    {
        RegisterHandler(pattern, handler);

        await ExecuteManyReplies(Join("PSUBSCRIBE", pattern), 1, cancellationToken);
    }

    /// <summary>
    /// Subscribes to a sharded channel on a dedicated socket and invokes
    /// <paramref name="handler"/> for <c>smessage</c> deliveries.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SubscribeShardedAsync(
        string channel,
        Action<PubSubMessage> handler,
        CancellationToken cancellationToken = default)
    {
        RegisterHandler(channel, handler);

        await ExecuteManyReplies(Join("SSUBSCRIBE", channel), 1, cancellationToken);
    }

    /// <summary>Same as the action overload, with an async handler.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SubscribeShardedAsync(
        string channel,
        Func<PubSubMessage, Task> handler,
        CancellationToken cancellationToken = default)
    {
        RegisterHandler(channel, handler);

        await ExecuteManyReplies(Join("SSUBSCRIBE", channel), 1, cancellationToken);
    }

    /// <summary>
    /// Subscribes to key expiration events for the currently selected database.
    /// The event key is in <see cref="PubSubMessage.Payload"/>.
    /// </summary>
    public ValueTask SubscribeExpiredAsync(
        Action<PubSubMessage> handler,
        CancellationToken cancellationToken = default)
    {
        return SubscribeKeyEventAsync("expired", handler, cancellationToken);
    }

    /// <summary>Async-handler overload of <see cref="SubscribeExpiredAsync(Action{PubSubMessage}, CancellationToken)"/>.</summary>
    public ValueTask SubscribeExpiredAsync(
        Func<PubSubMessage, Task> handler,
        CancellationToken cancellationToken = default)
    {
        return SubscribeKeyEventAsync("expired", handler, cancellationToken);
    }

    /// <summary>
    /// Subscribes to key eviction events for the currently selected database.
    /// The event key is in <see cref="PubSubMessage.Payload"/>.
    /// </summary>
    public ValueTask SubscribeEvictedAsync(
        Action<PubSubMessage> handler,
        CancellationToken cancellationToken = default)
    {
        return SubscribeKeyEventAsync("evicted", handler, cancellationToken);
    }

    /// <summary>Async-handler overload of <see cref="SubscribeEvictedAsync(Action{PubSubMessage}, CancellationToken)"/>.</summary>
    public ValueTask SubscribeEvictedAsync(
        Func<PubSubMessage, Task> handler,
        CancellationToken cancellationToken = default)
    {
        return SubscribeKeyEventAsync("evicted", handler, cancellationToken);
    }

    private ValueTask SubscribeKeyEventAsync(
        string eventName,
        Action<PubSubMessage> handler,
        CancellationToken cancellationToken)
    {
        return SubscribeShardedAsync($"__keyevent@{_database}__:{eventName}", handler, cancellationToken);
    }

    private ValueTask SubscribeKeyEventAsync(
        string eventName,
        Func<PubSubMessage, Task> handler,
        CancellationToken cancellationToken)
    {
        return SubscribeShardedAsync($"__keyevent@{_database}__:{eventName}", handler, cancellationToken);
    }

    /// <summary>
    /// Yields deliveries from the subscriber socket. Opens one if needed.
    /// </summary>
    public async IAsyncEnumerable<PubSubMessage> ListenAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            yield return await ReadPubSubDeliveryAsync(cancellationToken);
        }
    }

    /// <summary>Sends <c>UNSUBSCRIBE</c>. No channels drops every exact subscription on this connection.</summary>
    public ValueTask<IReadOnlyList<RespValue>> UnsubscribeAsync(params string[] channels)
    {
        RemoveHandlers(channels);

        return channels.Length == 0
            ? ExecuteManyReplies(["UNSUBSCRIBE"], 1)
            : ExecuteManyReplies(Join("UNSUBSCRIBE", channels), channels.Length);
    }

    /// <summary>Sends <c>PSUBSCRIBE</c> and reads one confirmation per pattern.</summary>
    public ValueTask<IReadOnlyList<RespValue>> SubscribePatternsAsync(params string[] patterns)
    {
        return ExecuteManyReplies(Join("PSUBSCRIBE", patterns), patterns.Length);
    }

    /// <summary>Sends <c>PUNSUBSCRIBE</c>.</summary>
    public ValueTask<IReadOnlyList<RespValue>> UnsubscribePatternsAsync(params string[] patterns)
    {
        RemoveHandlers(patterns);

        return patterns.Length == 0
            ? ExecuteManyReplies(["PUNSUBSCRIBE"], 1)
            : ExecuteManyReplies(Join("PUNSUBSCRIBE", patterns), patterns.Length);
    }

    /// <summary>Sends <c>SSUBSCRIBE</c> and reads one confirmation per channel.</summary>
    public ValueTask<IReadOnlyList<RespValue>> SubscribeShardedAsync(params string[] channels)
    {
        return ExecuteManyReplies(Join("SSUBSCRIBE", channels), channels.Length);
    }

    /// <summary>Sends <c>SUNSUBSCRIBE</c>.</summary>
    public ValueTask<IReadOnlyList<RespValue>> UnsubscribeShardedAsync(params string[] channels)
    {
        RemoveHandlers(channels);

        return channels.Length == 0
            ? ExecuteManyReplies(["SUNSUBSCRIBE"], 1)
            : ExecuteManyReplies(Join("SUNSUBSCRIBE", channels), channels.Length);
    }

    /// <summary>
    /// Reads the next RESP value without sending a command. Use this after a
    /// subscribe for <c>message</c>, <c>pmessage</c>, and <c>smessage</c> pushes.
    /// A delivery is an array; <see cref="RespValue.AsString"/> on that array is
    /// null. Prefer <see cref="ReadPubSubMessageAsync"/>.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<RespValue> ReadMessageAsync(CancellationToken cancellationToken = default)
    {
        RespValue reply = await ReadFromSubscriptionAsync(cancellationToken);

        return RuvioSocket.ThrowOnError(reply);
    }

    /// <summary>
    /// Reads until a <c>message</c>, <c>pmessage</c>, or <c>smessage</c> arrives,
    /// skipping subscribe confirmations.
    /// </summary>
    public ValueTask<PubSubMessage> ReadPubSubMessageAsync(CancellationToken cancellationToken = default)
    {
        return new ValueTask<PubSubMessage>(ReadPubSubDeliveryAsync(cancellationToken));
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<IReadOnlyList<RespValue>> ExecuteManyReplies(
        IReadOnlyList<string> arguments,
        int replies,
        CancellationToken cancellationToken = default)
    {
        if (replies < 1)
        {
            throw new ArgumentException("A subscribe command needs at least one channel or pattern.");
        }

        IReadOnlyList<RespValue> values = await ExecuteOnConnectionAsync(arguments, replies, cancellationToken);

        TrackSubscription(arguments);

        foreach (RespValue reply in values)
        {
            if (reply.Kind is RespKind.Error)
            {
                throw new RuvioException(reply.Text ?? "ERR", isServerReply: true);
            }
        }

        return values;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<long> Integer(RespCommand arguments, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(arguments, cancellationToken);

        if (reply.Kind is not RespKind.Integer)
        {
            throw new RuvioException($"{arguments.Name} returned {reply.Kind}");
        }

        return reply.Integer;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<bool> Flag(RespCommand arguments, CancellationToken cancellationToken = default)
    {
        return await Integer(arguments, cancellationToken) > 0;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask Ok(RespCommand arguments, CancellationToken cancellationToken = default)
    {
        await ExecuteCommandAsync(arguments, cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<string?> BulkOrNull(RespCommand arguments, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(arguments, cancellationToken);

        if (reply.Kind is RespKind.Null)
        {
            return null;
        }

        return reply.AsString();
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<IReadOnlyList<string>> ArrayOfBulks(RespCommand arguments, CancellationToken cancellationToken = default)
    {
        string?[] values = Bulks(await ExecuteCommandAsync(arguments, cancellationToken), arguments.Name);

        foreach (string? value in values)
        {
            if (value is null)
            {
                throw new RuvioException($"{arguments.Name} returned a null bulk");
            }
        }

        return values!;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<IReadOnlyList<string?>> ArrayOfNullableBulks(RespCommand arguments, CancellationToken cancellationToken = default)
    {
        return Bulks(await ExecuteCommandAsync(arguments, cancellationToken), arguments.Name);
    }

    private static string?[] Bulks(RespValue reply, string? name)
    {
        if (reply.Kind is RespKind.Null)
        {
            return [];
        }

        if (reply.Kind is not RespKind.Array || reply.Items is not { } items)
        {
            throw new RuvioException($"{name} returned {reply.Kind}");
        }

        var values = new string?[items.Count];

        for (int index = 0; index < values.Length; index++)
        {
            RespValue item = items[index];
            values[index] = item.Kind is RespKind.Null ? null : item.AsString();
        }

        return values;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<KeyValuePair<string, string>?> BlockPop(
        string command,
        IReadOnlyList<string> keys,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var arguments = new List<string> { command };

        arguments.AddRange(keys);
        arguments.Add(Number((long)Math.Ceiling(timeout.TotalSeconds)));
        RespValue reply = await ExecuteAsync(arguments, cancellationToken);

        if (reply.Kind is RespKind.Null)
        {
            return null;
        }

        if (reply.Items is not { Count: 2 })
        {
            throw new RuvioException($"{command} returned {reply.Kind}");
        }

        return new KeyValuePair<string, string>(
            reply.Items[0].AsString() ?? "",
            reply.Items[1].AsString() ?? "");
    }

    private static ScanPage ParseScan(RespValue reply)
    {
        if (reply.Items is not { Count: 2 })
        {
            throw new RuvioException("SCAN returned an unexpected shape");
        }

        long cursor = long.Parse(reply.Items[0].AsString() ?? "0", CultureInfo.InvariantCulture);
        IReadOnlyList<RespValue> items = reply.Items[1].Items ?? [];
        var keys = new string[items.Count];

        for (int index = 0; index < keys.Length; index++)
        {
            keys[index] = items[index].AsString() ?? "";
        }

        return new ScanPage(cursor, keys);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<(string Key, SortedSetEntry Entry)?> BlockPopSorted(
        string command,
        IReadOnlyList<string> keys,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var arguments = new List<string> { command };

        arguments.AddRange(keys);
        arguments.Add(Number((long)Math.Ceiling(timeout.TotalSeconds)));
        RespValue reply = await ExecuteAsync(arguments, cancellationToken);

        if (reply.Kind is RespKind.Null)
        {
            return null;
        }

        if (reply.Items is not { Count: 3 } items)
        {
            throw new RuvioException($"{command} returned {reply.Kind}");
        }

        return (
            items[0].AsString() ?? "",
            new SortedSetEntry(items[1].AsString() ?? "", ParseDouble(items[2].AsString() ?? "0")));
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<IReadOnlyList<long>> IntegerArray(RespCommand arguments)
    {
        RespValue reply = await ExecuteCommandAsync(arguments, CancellationToken.None);

        if (reply.Items is not { } items)
        {
            throw new RuvioException($"{arguments.Name} returned {reply.Kind}");
        }

        var values = new long[items.Count];

        for (int index = 0; index < values.Length; index++)
        {
            if (items[index].Kind is not RespKind.Integer)
            {
                throw new RuvioException($"{arguments.Name} returned {reply.Kind}");
            }

            values[index] = items[index].Integer;
        }

        return values;
    }

    /// <summary>Appends every stream key, then every id, as XREAD expects.</summary>
    private static void AddStreams(List<string> arguments, (string Key, string Id)[] streams)
    {
        arguments.EnsureCapacity(arguments.Count + (streams.Length * 2));

        foreach ((string key, string _) in streams)
        {
            arguments.Add(key);
        }

        foreach ((string _, string id) in streams)
        {
            arguments.Add(id);
        }
    }

    /// <summary>Appends an optional <c>NX</c>/<c>XX</c>/<c>GT</c>/<c>LT</c>, then <c>FIELDS numfields field ...</c>.</summary>
    private static string[] FieldBlock(string[] head, FieldExpireCondition condition, string[] fields)
    {
        string? flag = condition switch
        {
            FieldExpireCondition.IfNoTtl => "NX",
            FieldExpireCondition.IfTtl => "XX",
            FieldExpireCondition.IfGreater => "GT",
            FieldExpireCondition.IfLess => "LT",
            _ => null,
        };

        var arguments = new string[head.Length + (flag is null ? 2 : 3) + fields.Length];
        int cursor = head.Length;

        head.CopyTo(arguments, 0);

        if (flag is not null)
        {
            arguments[cursor++] = flag;
        }

        arguments[cursor++] = "FIELDS";
        arguments[cursor++] = Number(fields.Length);
        fields.CopyTo(arguments, cursor);

        return arguments;
    }

    private static IReadOnlyList<SortedSetEntry> ParseSortedSet(RespValue reply)
    {
        IReadOnlyList<RespValue> items = reply.Items ?? [];

        if (items.Count % 2 != 0)
        {
            throw new RuvioException("sorted set reply is not member/score pairs");
        }

        var entries = new List<SortedSetEntry>(items.Count / 2);

        for (int index = 0; index < items.Count; index += 2)
        {
            entries.Add(new SortedSetEntry(
                items[index].AsString() ?? "",
                ParseDouble(items[index + 1].AsString() ?? "0")));
        }

        return entries;
    }

    private static IReadOnlyList<StreamEntry> ParseStreamEntries(RespValue reply)
    {
        if (reply.Kind is RespKind.Null || reply.Items is null)
        {
            return [];
        }

        var entries = new List<StreamEntry>();

        foreach (RespValue item in reply.Items)
        {
            if (item.Items is not { Count: 2 })
            {
                continue;
            }

            string id = item.Items[0].AsString() ?? "";
            IReadOnlyDictionary<string, string> fields = Pairs(item.Items[1].Items);

            entries.Add(new StreamEntry(id, [.. fields]));
        }

        return entries;
    }

    private static IReadOnlyList<StreamReadResult> ParseStreamRead(RespValue reply)
    {
        if (reply.Kind is RespKind.Null || reply.Items is null)
        {
            return [];
        }

        var results = new List<StreamReadResult>();

        foreach (RespValue item in reply.Items)
        {
            if (item.Items is not { Count: 2 })
            {
                continue;
            }

            string key = item.Items[0].AsString() ?? "";

            results.Add(new StreamReadResult(key, ParseStreamEntries(item.Items[1])));
        }

        return results;
    }

    private static IReadOnlyDictionary<string, string> Pairs(IReadOnlyList<RespValue>? items)
    {
        items ??= [];
        var map = new Dictionary<string, string>();

        for (int index = 0; index + 1 < items.Count; index += 2)
        {
            map[items[index].AsString() ?? ""] = items[index + 1].AsString() ?? "";
        }

        return map;
    }

    private static IReadOnlyDictionary<string, string> PairsFromFlat(IReadOnlyList<string> items)
    {
        var map = new Dictionary<string, string>();

        for (int index = 0; index + 1 < items.Count; index += 2)
        {
            map[items[index]] = items[index + 1];
        }

        return map;
    }

    private static string Number(long value) => value.ToString(CultureInfo.InvariantCulture);

    private static string Number(double value) => value.ToString("G", CultureInfo.InvariantCulture);

    private static double ParseDouble(string text) =>
        double.Parse(text, CultureInfo.InvariantCulture);

    private static string[] Join(string command, params string[] arguments)
    {
        var values = new string[arguments.Length + 1];

        values[0] = command;
        arguments.CopyTo(values, 1);

        return values;
    }

    private static string[] Join(string command, string key, params string[] arguments)
    {
        var values = new string[arguments.Length + 2];

        values[0] = command;
        values[1] = key;
        arguments.CopyTo(values, 2);

        return values;
    }

    private static string[] Join(string command, string first, string second, params string[] arguments)
    {
        var values = new string[arguments.Length + 3];

        values[0] = command;
        values[1] = first;
        values[2] = second;
        arguments.CopyTo(values, 3);

        return values;
    }

    private static string[] FlattenPairs(string command, params (string Left, string Right)[] pairs)
    {
        var values = new string[1 + pairs.Length * 2];

        values[0] = command;
        int index = 1;

        foreach ((string left, string right) in pairs)
        {
            values[index++] = left;
            values[index++] = right;
        }

        return values;
    }

    private static string[] FlattenPairs(string command, string key, params (string Left, string Right)[] pairs)
    {
        var values = new string[2 + pairs.Length * 2];

        values[0] = command;
        values[1] = key;
        int index = 2;

        foreach ((string left, string right) in pairs)
        {
            values[index++] = left;
            values[index++] = right;
        }

        return values;
    }

    private static string[] FlattenPairs(string command, string key, string id, params (string Left, string Right)[] pairs)
    {
        var values = new string[3 + pairs.Length * 2];

        values[0] = command;
        values[1] = key;
        values[2] = id;
        int index = 3;

        foreach ((string left, string right) in pairs)
        {
            values[index++] = left;
            values[index++] = right;
        }

        return values;
    }
}
