using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
#pragma warning disable CS1591 // Command docs live on RuvioClient. This interface mirrors those signatures.

namespace Ruvio.Client;

/// <summary>
/// Commands on one logical database of a connected <see cref="IRuvioConnection"/>.
/// The connection owns the sockets and disposes every client it creates.
/// </summary>
public partial interface IRuvioClient
{
    /// <summary>The logical database currently selected by this client.</summary>
    int Database { get; }

    /// <summary>Host configured for this database client's command connection.</summary>
    string Host { get; }

    /// <summary>RESP port configured for this database client's command connection.</summary>
    int Port { get; }

    /// <summary>A published or pattern message delivered on the subscriber socket.</summary>
    event EventHandler<PubSubMessage>? Message;

    ValueTask<string> PingAsync(CancellationToken cancellationToken = default);

    ValueTask<string> PingAsync(string message, CancellationToken cancellationToken = default);

    ValueTask<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<string?> GetStringAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default);

    ValueTask<int?> GetInt32Async(string key, CancellationToken cancellationToken = default);

    ValueTask<long?> GetInt64Async(string key, CancellationToken cancellationToken = default);

    ValueTask<bool> SetAsync(string key, string value, CancellationToken cancellationToken = default);

    ValueTask<bool> SetAsync(string key, string value, TimeSpan? expiry, SetMode mode = SetMode.Always, CancellationToken cancellationToken = default);

    ValueTask<bool> SetAsync(string key, string value, DateTimeOffset expiresAt, SetMode mode = SetMode.Always, CancellationToken cancellationToken = default);

    ValueTask<bool> SetKeepingTimeToLiveAsync(string key, string value, SetMode mode = SetMode.Always, CancellationToken cancellationToken = default);

    ValueTask<string?> SetAndGetAsync(string key, string value, TimeSpan? expiry = null, SetMode mode = SetMode.Always, CancellationToken cancellationToken = default);

    ValueTask<long> IncrementAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<bool> ExpireAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default);

    ValueTask<long> DeleteAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<long> DeleteAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default);

    ValueTask<RespValue> ExecuteAsync(params string[] arguments);

    ValueTask<RespValue> ExecuteAsync(IReadOnlyList<string> arguments, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends raw RESP bulk arguments without text conversion. Implementations without binary
    /// support fail explicitly. The caller must not modify argument memory until completion.
    /// </summary>
    ValueTask<RespValue> ExecuteBinaryAsync(IReadOnlyList<ReadOnlyMemory<byte>> arguments, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("This client does not implement binary commands.");

    ValueTask<IReadOnlyList<RespValue>> ExecuteManyAsync(IReadOnlyList<IReadOnlyList<string>> commands, CancellationToken cancellationToken = default);

    ValueTask<long?> LimitAsync(string key, long max, long windowMs, CancellationToken cancellationToken = default);

    ValueTask<long> OnceAsync(string key, long ttlMs, CancellationToken cancellationToken = default);
    ValueTask<RespValue> BeginIdempotencyAsync(string key, string fingerprint, string owner, long ttlMs, CancellationToken cancellationToken = default);
    ValueTask<bool> CompleteIdempotencyAsync(string key, string fingerprint, string owner, string result, CancellationToken cancellationToken = default);

    ValueTask<long?> TakeAsync(string key, long count, CancellationToken cancellationToken = default);

    ValueTask<string?> GetExpireAsync(string key, long ttlMs, CancellationToken cancellationToken = default);

    ValueTask<long?> LeaseAsync(string key, long ttlMs, CancellationToken cancellationToken = default);

    ValueTask<long?> AcquireSemaphoreAsync(string key, long max, long ttlMs, CancellationToken cancellationToken = default);

    ValueTask<long?> IncrementWithLimitAsync(string key, long delta, long limit, CancellationToken cancellationToken = default);

    ValueTask<long> ReleaseAsync(string key, long token, CancellationToken cancellationToken = default);

    ValueTask<long> SetVersionAsync(string key, string value, CancellationToken cancellationToken = default);

    ValueTask<long> SetVersionAsync(string key, string value, long version, CancellationToken cancellationToken = default);

    ValueTask<long> StartChangesAsync(CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<ChangeNote>> ReadChangesAsync(long cursor, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<string?>> GetManyAsync(params string[] keys);

    ValueTask SetManyAsync(params (string Key, string Value)[] pairs);

    ValueTask<string?> GetSetAsync(string key, string value, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<string?>> JsonGetManyAsync(string path, params string[] keys);

    ValueTask<string?> JsonGetAsync(string key, string path = "$", CancellationToken cancellationToken = default);

    ValueTask<bool> JsonSetAsync(string key, string path, string json, CancellationToken cancellationToken = default);

    ValueTask<bool> JsonMergeAsync(string key, string path, string patch, CancellationToken cancellationToken = default);

    ValueTask<long> JsonDeleteAsync(string key, string path = "$", CancellationToken cancellationToken = default);

    ValueTask<string?> JsonTypeAsync(string key, string path = "$", CancellationToken cancellationToken = default);

    ValueTask<long?> JsonStringLengthAsync(string key, string path = "$", CancellationToken cancellationToken = default);

    ValueTask<long?> JsonArrayLengthAsync(string key, string path = "$", CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<string>> JsonObjectKeysAsync(string key, string path = "$", CancellationToken cancellationToken = default);

    ValueTask<string?> JsonIncrementAsync(string key, string path, double by, CancellationToken cancellationToken = default);

    ValueTask<bool?> JsonToggleAsync(string key, string path = "$", CancellationToken cancellationToken = default);

    ValueTask<long?> JsonArrayAppendAsync(string key, string path, string json, CancellationToken cancellationToken = default);

    ValueTask<long> JsonClearAsync(string key, string path = "$", CancellationToken cancellationToken = default);

    /// <summary>Reads a JSON value and deserializes it to <typeparamref name="T"/>. Null when the key or path is absent. Uses <paramref name="options"/>, else the client's default serializer options.</summary>
    [RequiresUnreferencedCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    [RequiresDynamicCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    ValueTask<T?> JsonGetValueAsync<T>(string key, string path = "$", JsonSerializerOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Same as above with source-generated metadata (AOT/trim friendly).</summary>
    ValueTask<T?> JsonGetValueAsync<T>(string key, string path, JsonTypeInfo<T> typeInfo, CancellationToken cancellationToken = default);

    /// <summary>Serializes <paramref name="value"/> (a string, number, bool, array or object) to JSON and sets it at <paramref name="path"/>. A plain <c>"izmir"</c> is sent as <c>"izmir"</c> with quotes.</summary>
    [RequiresUnreferencedCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    [RequiresDynamicCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    ValueTask<bool> JsonSetValueAsync<T>(string key, string path, T value, JsonSerializerOptions? options = null, CancellationToken cancellationToken = default);

    ValueTask<bool> JsonSetValueAsync<T>(string key, string path, T value, JsonTypeInfo<T> typeInfo, CancellationToken cancellationToken = default);

    /// <summary>Serializes <paramref name="patch"/> and sends it as a <c>JSON.MERGE</c> (RFC 7396) patch.</summary>
    [RequiresUnreferencedCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    [RequiresDynamicCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    ValueTask<bool> JsonMergeValueAsync<T>(string key, string path, T patch, JsonSerializerOptions? options = null, CancellationToken cancellationToken = default);

    ValueTask<bool> JsonMergeValueAsync<T>(string key, string path, T patch, JsonTypeInfo<T> typeInfo, CancellationToken cancellationToken = default);

    /// <summary>Serializes <paramref name="value"/> and appends it to the array at <paramref name="path"/>. Returns the new length; null when absent.</summary>
    [RequiresUnreferencedCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    [RequiresDynamicCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    ValueTask<long?> JsonArrayAppendValueAsync<T>(string key, string path, T value, JsonSerializerOptions? options = null, CancellationToken cancellationToken = default);

    ValueTask<long?> JsonArrayAppendValueAsync<T>(string key, string path, T value, JsonTypeInfo<T> typeInfo, CancellationToken cancellationToken = default);

    ValueTask<long> AppendAsync(string key, string value, CancellationToken cancellationToken = default);

    ValueTask<long> GetStringLengthAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<long> DecrementAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<long> IncrementByAsync(string key, long delta, CancellationToken cancellationToken = default);

    ValueTask<long> DecrementByAsync(string key, long delta, CancellationToken cancellationToken = default);

    ValueTask<long> UnlinkAsync(params string[] keys);

    ValueTask<long> ExistsAsync(params string[] keys);

    ValueTask<string> GetKeyTypeAsync(string key, CancellationToken cancellationToken = default);

    ValueTask RenameAsync(string key, string newKey, CancellationToken cancellationToken = default);

    ValueTask<ScanPage> ScanAsync(long cursor, string? pattern = null, int? count = null, CancellationToken cancellationToken = default);

    ValueTask<long> GetDatabaseSizeAsync(CancellationToken cancellationToken = default);

    ValueTask<bool> ExpireAtAsync(string key, DateTimeOffset when, CancellationToken cancellationToken = default);

    ValueTask<long> GetTimeToLiveAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<long> GetTimeToLiveMillisecondsAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<bool> PersistAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<long> PushListLeftAsync(string key, params string[] values);

    ValueTask<long> PushListRightAsync(string key, params string[] values);

    ValueTask<string?> PopListLeftAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<string>> PopListLeftAsync(string key, long count, CancellationToken cancellationToken = default);

    ValueTask<string?> PopListRightAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<string>> PopListRightAsync(string key, long count, CancellationToken cancellationToken = default);

    ValueTask<long> GetListLengthAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<string?> GetListItemAsync(string key, long index, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<string>> GetListRangeAsync(string key, long start, long stop, CancellationToken cancellationToken = default);

    ValueTask TrimListAsync(string key, long start, long stop, CancellationToken cancellationToken = default);

    ValueTask<KeyValuePair<string, string>?> PopListLeftBlockingAsync(string key, TimeSpan timeout, CancellationToken cancellationToken = default);

    ValueTask<KeyValuePair<string, string>?> PopListLeftBlockingAsync(IReadOnlyList<string> keys, TimeSpan timeout, CancellationToken cancellationToken = default);

    ValueTask<KeyValuePair<string, string>?> PopListRightBlockingAsync(string key, TimeSpan timeout, CancellationToken cancellationToken = default);

    ValueTask<KeyValuePair<string, string>?> PopListRightBlockingAsync(IReadOnlyList<string> keys, TimeSpan timeout, CancellationToken cancellationToken = default);

    ValueTask<long> AddSetMembersAsync(string key, params string[] members);

    ValueTask<long> RemoveSetMembersAsync(string key, params string[] members);

    ValueTask<bool> SetContainsAsync(string key, string member, CancellationToken cancellationToken = default);

    ValueTask<long> GetSetCountAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<string>> GetSetMembersAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<string>> GetSetIntersectionAsync(params string[] keys);

    ValueTask<IReadOnlyList<string>> GetSetUnionAsync(params string[] keys);

    ValueTask<IReadOnlyList<string>> GetSetDifferenceAsync(params string[] keys);

    ValueTask<long> StoreSetIntersectionAsync(string destination, params string[] keys);

    ValueTask<long> StoreSetUnionAsync(string destination, params string[] keys);

    ValueTask<long> StoreSetDifferenceAsync(string destination, params string[] keys);

    ValueTask<bool> MoveSetMemberAsync(string source, string destination, string member, CancellationToken cancellationToken = default);

    ValueTask<string?> PopSetMemberAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<string>> PopSetMembersAsync(string key, long count, CancellationToken cancellationToken = default);

    ValueTask<string?> GetSetRandomMemberAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<string>> GetSetRandomMembersAsync(string key, long count, CancellationToken cancellationToken = default);

    ValueTask<long> SetHashFieldsAsync(string key, params (string Field, string Value)[] fields);

    ValueTask<string?> GetHashFieldAsync(string key, string field, CancellationToken cancellationToken = default);

    ValueTask<long> DeleteHashFieldsAsync(string key, params string[] fields);

    ValueTask<long> GetHashLengthAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyDictionary<string, string>> GetHashAllAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<string?>> GetHashFieldsAsync(string key, params string[] fields);

    ValueTask<bool> HashFieldExistsAsync(string key, string field, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<string>> GetHashFieldNamesAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<string>> GetHashFieldValuesAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<long> IncrementHashFieldAsync(string key, string field, long delta, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<long>> ExpireHashFieldsAsync(string key, TimeSpan ttl, params string[] fields);

    ValueTask<IReadOnlyList<long>> ExpireHashFieldsAsync(string key, TimeSpan ttl, FieldExpireCondition condition, params string[] fields);

    ValueTask<IReadOnlyList<long>> ExpireHashFieldsAtAsync(string key, DateTimeOffset when, params string[] fields);

    ValueTask<IReadOnlyList<long>> GetHashFieldTimeToLiveAsync(string key, params string[] fields);

    ValueTask<IReadOnlyList<long>> GetHashFieldTimeToLiveMillisecondsAsync(string key, params string[] fields);

    ValueTask<IReadOnlyList<long>> GetHashFieldExpireTimeAsync(string key, params string[] fields);

    ValueTask<IReadOnlyList<long>> GetHashFieldExpireTimeMillisecondsAsync(string key, params string[] fields);

    ValueTask<IReadOnlyList<long>> PersistHashFieldsAsync(string key, params string[] fields);

    ValueTask<bool> SetHashFieldIfAbsentAsync(string key, string field, string value, CancellationToken cancellationToken = default);

    ValueTask<long> GetHashFieldLengthAsync(string key, string field, CancellationToken cancellationToken = default);

    ValueTask<HashScanPage> ScanHashAsync(string key, long cursor, CancellationToken cancellationToken = default);

    ValueTask<HashScanPage> ScanHashAsync(string key, long cursor, string? pattern, int? count = null, CancellationToken cancellationToken = default);

    ValueTask<long> AddSortedSetAsync(string key, params SortedSetEntry[] entries);

    ValueTask<bool> AddSortedSetAsync(string key, string member, double score, CancellationToken cancellationToken = default);

    ValueTask<long> AddSortedSetAsync(string key, params (string Member, double Score)[] entries);

    ValueTask<long> AddSortedSetAsync(string key, SortedSetAddFlags flags, params (string Member, double Score)[] entries);

    ValueTask<long> AddSortedSetAsync(string key, SortedSetAddFlags flags, params SortedSetEntry[] entries);

    ValueTask<double?> IncrementSortedSetAsync(string key, string member, double delta, SortedSetAddFlags flags, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<string>> GetSortedSetRangeByRankAsync(string key, long start, long stop, bool descending = false, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetRangeByRankWithScoresAsync(string key, long start, long stop, bool descending = false, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<string>> GetSortedSetRangeByLexAsync(string key, string min, string max, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetRangeByScoreWithScoresAsync(string key, string min, string max, CancellationToken cancellationToken = default);

    ValueTask<long> RemoveSortedSetMembersAsync(string key, params string[] members);

    ValueTask<IReadOnlyList<SortedSetEntry>> PopSortedSetMinAsync(string key, long count = 1, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<SortedSetEntry>> PopSortedSetMaxAsync(string key, long count = 1, CancellationToken cancellationToken = default);

    ValueTask<(string Key, SortedSetEntry Entry)?> PopSortedSetMinBlockingAsync(string key, TimeSpan timeout, CancellationToken cancellationToken = default);

    ValueTask<(string Key, SortedSetEntry Entry)?> PopSortedSetMinBlockingAsync(IReadOnlyList<string> keys, TimeSpan timeout, CancellationToken cancellationToken = default);

    ValueTask<(string Key, SortedSetEntry Entry)?> PopSortedSetMaxBlockingAsync(string key, TimeSpan timeout, CancellationToken cancellationToken = default);

    ValueTask<(string Key, SortedSetEntry Entry)?> PopSortedSetMaxBlockingAsync(IReadOnlyList<string> keys, TimeSpan timeout, CancellationToken cancellationToken = default);

    ValueTask<long> RemoveSortedSetRangeByRankAsync(string key, long start, long stop, CancellationToken cancellationToken = default);

    ValueTask<long> RemoveSortedSetRangeByScoreAsync(string key, string min, string max, CancellationToken cancellationToken = default);

    ValueTask<long> RemoveSortedSetRangeByLexAsync(string key, string min, string max, CancellationToken cancellationToken = default);

    ValueTask<double> IncrementSortedSetAsync(string key, string member, double delta, CancellationToken cancellationToken = default);

    ValueTask<long> GetSortedSetCountAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<double?> GetSortedSetScoreAsync(string key, string member, CancellationToken cancellationToken = default);

    ValueTask<long> AddGeoAsync(string key, params (double Longitude, double Latitude, string Member)[] places);

    ValueTask<double?> GeoDistanceAsync(string key, string from, string to, string unit = "m", CancellationToken cancellationToken = default);

    /// <summary>Sends <c>GEODIST</c> with a typed unit. Example: <c>GeoDistanceAsync("cities", "a", "b", GeoUnit.Kilometers)</c>.</summary>
    ValueTask<double?> GeoDistanceAsync(string key, string from, string to, GeoUnit unit, CancellationToken cancellationToken = default)
        => RespRead.OptionalDouble(ExecuteAsync(new string[] { "GEODIST", key, from, to, Keywords.Word(unit) }, cancellationToken), "GEODIST");

    ValueTask<IReadOnlyList<string?>> GeoHashAsync(string key, params string[] members);

    ValueTask<IReadOnlyList<(double Longitude, double Latitude)?>> GeoPositionAsync(string key, params string[] members);

    ValueTask<IReadOnlyList<string>> GeoSearchAsync(string key, double longitude, double latitude, double radius, string unit);

    ValueTask<long> GeoSearchStoreAsync(string destination, string source, string member, double radius, string unit);

    ValueTask<IReadOnlyList<string>> GeoRadiusByMemberAsync(string key, string member, double radius, string unit);

    ValueTask<IReadOnlyList<string>> GeoRadiusAsync(string key, double longitude, double latitude, double radius, string unit);

    /// <summary>Sends <c>GEOSEARCH</c> with a typed unit and result order (nearest first by default).</summary>
    ValueTask<IReadOnlyList<string>> GeoSearchAsync(string key, double longitude, double latitude, double radius, GeoUnit unit, GeoOrder order = GeoOrder.Ascending)
        => RespRead.Strings(ExecuteAsync(new string[] { "GEOSEARCH", key, "FROMLONLAT", RespRead.Number(longitude), RespRead.Number(latitude), "BYRADIUS", RespRead.Number(radius), Keywords.Word(unit), Keywords.Word(order) }, CancellationToken.None), "GEOSEARCH");

    /// <summary>Sends <c>GEOSEARCHSTORE</c> with a typed unit.</summary>
    ValueTask<long> GeoSearchStoreAsync(string destination, string source, string member, double radius, GeoUnit unit)
        => RespRead.Integer(ExecuteAsync(new string[] { "GEOSEARCHSTORE", destination, source, "FROMMEMBER", member, "BYRADIUS", RespRead.Number(radius), Keywords.Word(unit) }, CancellationToken.None), "GEOSEARCHSTORE");

    /// <summary>Sends <c>GEORADIUSBYMEMBER</c> with a typed unit and result order (nearest first by default).</summary>
    ValueTask<IReadOnlyList<string>> GeoRadiusByMemberAsync(string key, string member, double radius, GeoUnit unit, GeoOrder order = GeoOrder.Ascending)
        => RespRead.Strings(ExecuteAsync(new string[] { "GEORADIUSBYMEMBER", key, member, RespRead.Number(radius), Keywords.Word(unit), Keywords.Word(order) }, CancellationToken.None), "GEORADIUSBYMEMBER");

    /// <summary>Sends <c>GEORADIUS</c> with a typed unit and result order (nearest first by default).</summary>
    ValueTask<IReadOnlyList<string>> GeoRadiusAsync(string key, double longitude, double latitude, double radius, GeoUnit unit, GeoOrder order = GeoOrder.Ascending)
        => RespRead.Strings(ExecuteAsync(new string[] { "GEORADIUS", key, RespRead.Number(longitude), RespRead.Number(latitude), RespRead.Number(radius), Keywords.Word(unit), Keywords.Word(order) }, CancellationToken.None), "GEORADIUS");

    ValueTask<long?> GetSortedSetRankAsync(string key, string member, bool descending = false, CancellationToken cancellationToken = default);

    ValueTask ReserveBloomFilterAsync(string key, double errorRate, long capacity, CancellationToken cancellationToken = default);

    ValueTask<bool> AddBloomFilterAsync(string key, string item, CancellationToken cancellationToken = default);

    ValueTask<bool> BloomFilterContainsAsync(string key, string item, CancellationToken cancellationToken = default);

    ValueTask<string?> AddStreamEntryIfExistsAsync(string key, string id, params (string Field, string Value)[] fields);

    ValueTask<string> AddStreamEntryAsync(string key, string id, params (string Field, string Value)[] fields);

    ValueTask<string> AddStreamEntryDelayedAsync(string key, long delayMs, string id, params (string Field, string Value)[] fields);

    ValueTask<string> AddStreamEntryAsync(string key, string id, long maxLength, params (string Field, string Value)[] fields);

    ValueTask<long> GetStreamLengthAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<RespValue> GetStreamInfoAsync(string key, CancellationToken cancellationToken = default);

    ValueTask<RespValue> GetStreamGroupsAsync(string key, CancellationToken cancellationToken = default);

    ValueTask SetStreamIdAsync(string key, string id, CancellationToken cancellationToken = default);

    ValueTask<RespValue> AutoClaimStreamAsync(string key, string group, string consumer, long minIdleMs, string start, CancellationToken cancellationToken = default);

    ValueTask<RespValue> GetStreamConsumersAsync(string key, string group, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<StreamEntry>> GetStreamRangeAsync(string key, string start, string end, bool reverse = false, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<StreamEntry>> GetStreamRangeAsync(string key, string start, string end, int count, bool reverse = false, CancellationToken cancellationToken = default);

    ValueTask<long> DeleteStreamEntriesAsync(string key, params string[] ids);

    ValueTask<long> TrimStreamMinIdAsync(string key, string id, CancellationToken cancellationToken = default);

    ValueTask<long> TrimStreamAsync(string key, long maxLength, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<StreamReadResult>> GetStreamUpdatesAsync(params (string Key, string Id)[] streams);

    ValueTask<IReadOnlyList<StreamReadResult>> GetStreamUpdatesAsync(StreamReadOptions options, params (string Key, string Id)[] streams);

    ValueTask<long> CreateStreamConsumerAsync(string key, string group, string consumer, CancellationToken cancellationToken = default);

    ValueTask CreateStreamGroupAsync(string key, string group, string id, CancellationToken cancellationToken = default);

    ValueTask CreateStreamGroupAsync(string key, string group, string id, bool createStream, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<StreamReadResult>> GetStreamGroupUpdatesAsync(string group, string consumer, string key, string id, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<StreamReadResult>> GetStreamGroupUpdatesAsync(string group, string consumer, string key, string id, StreamReadOptions options, CancellationToken cancellationToken = default);

    ValueTask<long> DestroyStreamGroupAsync(string key, string group, CancellationToken cancellationToken = default);

    ValueTask SetStreamGroupIdAsync(string key, string group, string id, CancellationToken cancellationToken = default);

    ValueTask<long> DeleteStreamGroupConsumerAsync(string key, string group, string consumer, CancellationToken cancellationToken = default);

    ValueTask<long> AcknowledgeStreamAsync(string key, string group, params string[] ids);

    ValueTask<RespValue> GetStreamPendingAsync(string key, string group, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<StreamPendingEntry>> GetStreamPendingAsync(string key, string group, string start, string end, long count, string? consumer = null, TimeSpan? minIdle = null, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<StreamEntry>> ClaimStreamEntriesAsync(string key, string group, string consumer, long minIdleMilliseconds, params string[] ids);

    ValueTask<IReadOnlyList<StreamEntry>> ClaimStreamEntriesAsync(string key, string group, string consumer, long minIdleMilliseconds, StreamClaimOptions options, params string[] ids);

    ValueTask<IReadOnlyList<string>> ClaimStreamEntryIdsAsync(string key, string group, string consumer, long minIdleMilliseconds, StreamClaimOptions? options, params string[] ids);

    ValueTask<string> GetInfoAsync(CancellationToken cancellationToken = default);

    ValueTask<string> GetInfoAsync(string section, CancellationToken cancellationToken = default);

    ValueTask SaveAsync(CancellationToken cancellationToken = default);

    ValueTask SaveInBackgroundAsync(CancellationToken cancellationToken = default);

    ValueTask<long> PublishAsync(string channel, string message, CancellationToken cancellationToken = default);

    ValueTask<long> PublishShardedAsync(string channel, string message, CancellationToken cancellationToken = default);

    ValueTask<RespValue> EvaluateAsync(string script, long keyCount, params string[] arguments);

    ValueTask BeginTransactionAsync(CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<RespValue>> ExecuteTransactionAsync(CancellationToken cancellationToken = default);

    ValueTask DiscardTransactionAsync(CancellationToken cancellationToken = default);

    ValueTask WatchAsync(params string[] keys);

    ValueTask UnwatchAsync(CancellationToken cancellationToken = default);

    ValueTask<string> GetAclUserNameAsync(CancellationToken cancellationToken = default);

    ValueTask<RespValue> GetClusterSlotsAsync(CancellationToken cancellationToken = default);

    ValueTask<string> GetClusterNodesAsync(CancellationToken cancellationToken = default);

    ValueTask<RespValue> GetClusterShardsAsync(CancellationToken cancellationToken = default);

    ValueTask<string> GetClusterInfoAsync(CancellationToken cancellationToken = default);

    ValueTask<string> GetClusterMyIdAsync(CancellationToken cancellationToken = default);

    ValueTask<long> GetClusterKeySlotAsync(string key, CancellationToken cancellationToken = default);

    ValueTask ReadOnlyAsync(CancellationToken cancellationToken = default);

    ValueTask ReadWriteAsync(CancellationToken cancellationToken = default);

    ValueTask<string> LoadScriptAsync(string script, CancellationToken cancellationToken = default);

    ValueTask<RespValue> EvaluateShaAsync(string sha, long keyCount, params string[] arguments);

    ValueTask<IReadOnlyList<bool>> ScriptExistsAsync(params string[] hashes);

    ValueTask FlushScriptsAsync(CancellationToken cancellationToken = default);

    ValueTask KillScriptAsync(CancellationToken cancellationToken = default);

    ValueTask<RespValue> CallFunctionAsync(string name, long keyCount, params string[] arguments);

    ValueTask LoadFunctionAsync(string name, string body, CancellationToken cancellationToken = default);

    ValueTask<RespValue> GetFunctionListAsync(CancellationToken cancellationToken = default);

    ValueTask<bool> DeleteFunctionAsync(string name, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<string>> GetAclUsersAsync(CancellationToken cancellationToken = default);

    ValueTask<string?> GetConnectionNameAsync(CancellationToken cancellationToken = default);

    ValueTask SetConnectionNameAsync(string name, CancellationToken cancellationToken = default);

    ValueTask SetTrackingAsync(bool enabled, CancellationToken cancellationToken = default);

    ValueTask<RespValue> GetConfigAsync(string parameter, CancellationToken cancellationToken = default);

    ValueTask FlushDatabaseAsync(CancellationToken cancellationToken = default);

    ValueTask FlushAllDatabasesAsync(CancellationToken cancellationToken = default);

    ValueTask SwapDatabasesAsync(int first, int second, CancellationToken cancellationToken = default);

    ValueTask<bool> MoveKeyAsync(string key, int database, CancellationToken cancellationToken = default);

    ValueTask<RespValue> HelloAsync(int protocol = 2, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<string>> GetAclListAsync(CancellationToken cancellationToken = default);

    ValueTask<RespValue> GetAclUserAsync(string user, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<string>> GetAclCategoriesAsync(CancellationToken cancellationToken = default);

    ValueTask SetAclUserAsync(string user, params string[] rules);

    ValueTask LoadAclAsync(CancellationToken cancellationToken = default);

    ValueTask SaveAclAsync(CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<RespValue>> SubscribeAsync(params string[] channels);

    ValueTask SubscribeAsync(string channel, Action<PubSubMessage> handler, CancellationToken cancellationToken = default);

    ValueTask SubscribeAsync(string channel, Func<PubSubMessage, Task> handler, CancellationToken cancellationToken = default);

    ValueTask SubscribePatternsAsync(string pattern, Action<PubSubMessage> handler, CancellationToken cancellationToken = default);

    ValueTask SubscribePatternsAsync(string pattern, Func<PubSubMessage, Task> handler, CancellationToken cancellationToken = default);

    ValueTask SubscribeShardedAsync(string channel, Action<PubSubMessage> handler, CancellationToken cancellationToken = default);

    ValueTask SubscribeShardedAsync(string channel, Func<PubSubMessage, Task> handler, CancellationToken cancellationToken = default);

    ValueTask SubscribeExpiredAsync(
        Action<PubSubMessage> handler,
        CancellationToken cancellationToken = default) =>
        SubscribeShardedAsync($"__keyevent@{Database}__:expired", handler, cancellationToken);

    ValueTask SubscribeExpiredAsync(
        Func<PubSubMessage, Task> handler,
        CancellationToken cancellationToken = default) =>
        SubscribeShardedAsync($"__keyevent@{Database}__:expired", handler, cancellationToken);

    ValueTask SubscribeEvictedAsync(
        Action<PubSubMessage> handler,
        CancellationToken cancellationToken = default) =>
        SubscribeShardedAsync($"__keyevent@{Database}__:evicted", handler, cancellationToken);

    ValueTask SubscribeEvictedAsync(
        Func<PubSubMessage, Task> handler,
        CancellationToken cancellationToken = default) =>
        SubscribeShardedAsync($"__keyevent@{Database}__:evicted", handler, cancellationToken);

    IAsyncEnumerable<PubSubMessage> ListenAsync(CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<RespValue>> UnsubscribeAsync(params string[] channels);

    ValueTask<IReadOnlyList<RespValue>> SubscribePatternsAsync(params string[] patterns);

    ValueTask<IReadOnlyList<RespValue>> UnsubscribePatternsAsync(params string[] patterns);

    ValueTask<IReadOnlyList<RespValue>> SubscribeShardedAsync(params string[] channels);

    ValueTask<IReadOnlyList<RespValue>> UnsubscribeShardedAsync(params string[] channels);

    ValueTask<RespValue> ReadMessageAsync(CancellationToken cancellationToken = default);

    ValueTask<PubSubMessage> ReadPubSubMessageAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>KEYS</c>. The server refuses to return more than 100,000 keys.</summary>
    ValueTask<IReadOnlyList<string>> GetKeysAsync(string pattern, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>RANDOMKEY</c>. Null when the database is empty.</summary>
    ValueTask<string?> GetRandomKeyAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>TOUCH</c> and returns how many of the keys exist.</summary>
    ValueTask<long> TouchAsync(params string[] keys);

    /// <summary>Sends <c>RENAMENX</c>. False when the new key already exists.</summary>
    ValueTask<bool> RenameIfNotExistsAsync(string key, string newKey, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>EXPIRETIME</c>: unix seconds, -1 without a TTL, -2 for a missing key.</summary>
    ValueTask<long> GetExpireTimeAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>PEXPIRETIME</c>: unix milliseconds, -1 without a TTL, -2 for a missing key.</summary>
    ValueTask<long> GetExpireTimeMillisecondsAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>COPY</c>. False when the source is missing or the target exists and <paramref name="replace"/> is false.</summary>
    ValueTask<bool> CopyAsync(string source, string destination, bool replace = false, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ECHO</c>.</summary>
    ValueTask<string> EchoAsync(string message, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>OBJECT ENCODING</c>. Null for a missing key.</summary>
    ValueTask<string?> GetObjectEncodingAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>OBJECT IDLETIME</c> in seconds. Null for a missing key.</summary>
    ValueTask<long?> GetObjectIdleSecondsAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>OBJECT FREQ</c>. Null for a missing key.</summary>
    ValueTask<long?> GetObjectFrequencyAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>MEMORY USAGE</c>. Null for a missing key.</summary>
    ValueTask<long?> GetMemoryUsageAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SLOWLOG LEN</c>.</summary>
    ValueTask<long> GetSlowLogLengthAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SLOWLOG RESET</c>.</summary>
    ValueTask ResetSlowLogAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SLOWLOG GET</c> for up to <paramref name="count"/> entries. Each entry is returned as the raw reply.</summary>
    ValueTask<IReadOnlyList<RespValue>> GetSlowLogAsync(long count = 10, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CLIENT ID</c>.</summary>
    ValueTask<long> GetClientIdAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CLIENT INFO</c>: one line for this connection.</summary>
    ValueTask<string> GetClientInfoAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CLIENT LIST</c>: one line per connection on the answering shard.</summary>
    ValueTask<string> GetClientListAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CLIENT KILL ID</c> and returns how many connections were closed.</summary>
    ValueTask<long> KillClientAsync(long id, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LINSERT</c>. Returns the new length, -1 when the pivot is missing, 0 for a missing list.</summary>
    ValueTask<long> InsertListAsync(string key, bool before, string pivot, string value, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LINSERT</c> with a typed position. Example: <c>InsertListAsync("queue", InsertPosition.After, "b", "c")</c>.</summary>
    ValueTask<long> InsertListAsync(string key, InsertPosition position, string pivot, string value, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(new string[] { "LINSERT", key, Keywords.Word(position), pivot, value }, cancellationToken), "LINSERT");

    /// <summary>Sends <c>LSET</c>.</summary>
    ValueTask SetListItemAsync(string key, long index, string value, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LREM</c> and returns how many elements were removed.</summary>
    ValueTask<long> RemoveListItemsAsync(string key, long count, string value, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LPOS</c> for the first match. Null when the value is not in the list.</summary>
    ValueTask<long?> GetListPositionAsync(string key, string value, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LPUSHX</c>. Returns 0 and writes nothing when the list does not exist.</summary>
    ValueTask<long> PushListLeftIfExistsAsync(string key, params string[] values);

    /// <summary>Sends <c>RPUSHX</c>. Returns 0 and writes nothing when the list does not exist.</summary>
    ValueTask<long> PushListRightIfExistsAsync(string key, params string[] values);

    /// <summary>Sends <c>LMOVE</c>. Null when the source is empty.</summary>
    ValueTask<string?> MoveListItemAsync(string source, string destination, bool fromLeft, bool toLeft, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LMOVE</c> with typed sides. Example: <c>MoveListItemAsync("a", "b", ListSide.Right, ListSide.Left)</c>.</summary>
    ValueTask<string?> MoveListItemAsync(string source, string destination, ListSide from, ListSide to, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteAsync(new string[] { "LMOVE", source, destination, Keywords.Word(from), Keywords.Word(to) }, cancellationToken));

    /// <summary>Sends <c>RPOPLPUSH</c>. Null when the source is empty.</summary>
    ValueTask<string?> PopListRightPushLeftAsync(string source, string destination, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>BLMOVE</c>. Null when <paramref name="timeout"/> passes; <see cref="TimeSpan.Zero"/> waits forever.</summary>
    ValueTask<string?> MoveListItemBlockingAsync(string source, string destination, bool fromLeft, bool toLeft, TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>BLMOVE</c> with typed sides. Null when <paramref name="timeout"/> passes; <see cref="TimeSpan.Zero"/> waits forever.</summary>
    ValueTask<string?> MoveListItemBlockingAsync(string source, string destination, ListSide from, ListSide to, TimeSpan timeout, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteAsync(new string[] { "BLMOVE", source, destination, Keywords.Word(from), Keywords.Word(to), RespRead.Number((long)Math.Ceiling(timeout.TotalSeconds)) }, cancellationToken));

    /// <summary>Sends <c>LMPOP</c>. Returns the key that was popped and its elements, or null when every list is empty.</summary>
    ValueTask<KeyValuePair<string, IReadOnlyList<string>>?> PopListManyAsync(IReadOnlyList<string> keys, bool fromLeft, long count = 1, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LMPOP</c> with a typed side. Example: <c>PopListManyAsync(keys, ListSide.Left, 2)</c>.</summary>
    ValueTask<KeyValuePair<string, IReadOnlyList<string>>?> PopListManyAsync(IReadOnlyList<string> keys, ListSide side, long count = 1, CancellationToken cancellationToken = default)
        => RespRead.ListPop(ExecuteAsync(PopListManyCommand(keys, side, count), cancellationToken), "LMPOP");

    private static string[] PopListManyCommand(IReadOnlyList<string> keys, ListSide side, long count)
    {
        ArgumentNullException.ThrowIfNull(keys);

        var parts = new string[keys.Count + 5];

        parts[0] = "LMPOP";
        parts[1] = RespRead.Number(keys.Count);

        for (int index = 0; index < keys.Count; index++)
        {
            parts[index + 2] = keys[index];
        }

        parts[^3] = Keywords.Word(side);
        parts[^2] = "COUNT";
        parts[^1] = RespRead.Number(count);

        return parts;
    }

    /// <summary>Sends <c>BITFIELD</c>. Each result is a number, or null when <c>OVERFLOW FAIL</c> refused the write.</summary>
    ValueTask<IReadOnlyList<long?>> BitFieldAsync(string key, params string[] operations);

    /// <summary>Sends <c>BITFIELD_RO</c> with <c>GET type offset</c> subcommands.</summary>
    ValueTask<IReadOnlyList<long?>> BitFieldReadOnlyAsync(string key, params string[] operations);

    /// <summary>Sends <c>BITFIELD</c> from typed steps. Example: <c>BitFieldAsync("k", BitFieldStep.IncrBy(BitFieldType.Unsigned(8), 0, 1))</c>.</summary>
    ValueTask<IReadOnlyList<long?>> BitFieldAsync(string key, BitFieldStep first, params BitFieldStep[] rest)
        => RespRead.BitFieldResults(ExecuteAsync(BitFieldStep.Command("BITFIELD", key, first, rest), CancellationToken.None), "BITFIELD");

    /// <summary>Sends <c>BITFIELD_RO</c> from typed <see cref="BitFieldStep.Get"/> steps.</summary>
    ValueTask<IReadOnlyList<long?>> BitFieldReadOnlyAsync(string key, BitFieldStep first, params BitFieldStep[] rest)
        => RespRead.BitFieldResults(ExecuteAsync(BitFieldStep.Command("BITFIELD_RO", key, first, rest), CancellationToken.None), "BITFIELD_RO");

    /// <summary>Sends <c>DUMP</c>. The payload is opaque and readable only by <c>RESTORE</c> on a Ruvio server. Null for a missing key.</summary>
    ValueTask<byte[]?> DumpAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>RESTORE</c>. A key that exists is an error unless <paramref name="replace"/> is true.</summary>
    ValueTask RestoreAsync(string key, byte[] payload, long ttlMilliseconds = 0, bool replace = false, bool absoluteTtl = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>FILLGET</c>. One caller gets <see cref="FillState.Fill"/>; with <paramref name="wait"/> the server holds the others until it writes.
    /// A wait holds the whole connection, so commands sent after it (including the filler's <c>FILLSET</c> on a shared client)
    /// are processed only when it ends: use a connection of its own, or omit it and poll.
    /// </summary>
    ValueTask<FillResult> FillGetAsync(string key, TimeSpan fill, TimeSpan? wait = null, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>FILLSET</c>. True when <paramref name="token"/> was valid and the value was written.</summary>
    ValueTask<bool> FillSetAsync(string key, string value, TimeSpan ttl, long token, TimeSpan? stale = null, CancellationToken cancellationToken = default);
}
