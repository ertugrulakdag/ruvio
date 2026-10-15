// Typed methods for the commands and options added in the coverage pass. Frames are built by
// CoverageCommands; replies are read by RespRead with pooled state machines.

namespace Ruvio.Client;

public sealed partial class RuvioClient
{
    /// <summary>Sends <c>SET key value IFEQ expected [PX ms]</c> (Ruvio extension). True when the stored string equaled <paramref name="expected"/> and was replaced; false (nothing written) otherwise.</summary>
    public ValueTask<bool> SetIfEqualAsync(string key, string value, string expected, TimeSpan? expiry = null, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteCommandAsync(CoverageCommands.SetIfEqual(key, value, expected, expiry), cancellationToken));

    /// <summary>Sends <c>SET key value IFEQ expected KEEPTTL</c>: compare-and-set that keeps the current expiry.</summary>
    public ValueTask<bool> SetIfEqualKeepingTimeToLiveAsync(string key, string value, string expected, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteCommandAsync(CoverageCommands.SetIfEqualKeepingTimeToLive(key, value, expected), cancellationToken));

    /// <summary>Sends <c>DELEX key IFEQ expected</c> (Ruvio extension). True when the string equaled <paramref name="expected"/> and was deleted.</summary>
    public ValueTask<bool> DeleteIfEqualAsync(string key, string expected, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteCommandAsync(CoverageCommands.DeleteIfEqual(key, expected), cancellationToken), "DELEX");

    /// <summary>Sends <c>SETEX key seconds value</c>.</summary>
    public ValueTask SetExpireAsync(string key, long seconds, string value, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteCommandAsync(CoverageCommands.SetExpire(key, seconds, value), cancellationToken));

    /// <summary>Sends <c>PSETEX key milliseconds value</c>.</summary>
    public ValueTask SetExpireMillisecondsAsync(string key, long milliseconds, string value, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteCommandAsync(CoverageCommands.SetExpireMilliseconds(key, milliseconds, value), cancellationToken));

    /// <summary>Sends <c>SETNX</c>. True when the key was missing and was written.</summary>
    public ValueTask<bool> SetIfNotExistsAsync(string key, string value, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteCommandAsync(CoverageCommands.SetIfNotExists(key, value), cancellationToken), "SETNX");

    /// <summary>Sends <c>GETDEL</c>: returns the string and deletes the key. Null when missing.</summary>
    public ValueTask<string?> GetDeleteAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteCommandAsync(CoverageCommands.GetDelete(key), cancellationToken));

    /// <summary>Sends <c>INCRBYFLOAT</c> and returns the new value.</summary>
    public ValueTask<double> IncrementByFloatAsync(string key, double delta, CancellationToken cancellationToken = default)
        => RespRead.Double(ExecuteCommandAsync(CoverageCommands.IncrementByFloat(key, delta), cancellationToken), "INCRBYFLOAT");

    /// <summary>Sends <c>GETRANGE key start end</c> (inclusive, negative from the end).</summary>
    public ValueTask<string> GetRangeAsync(string key, long start, long end, CancellationToken cancellationToken = default)
        => RespRead.RequiredText(ExecuteCommandAsync(CoverageCommands.GetRange("GETRANGE", key, start, end), cancellationToken), "GETRANGE");

    /// <summary>Sends <c>SUBSTR</c>, the legacy alias of <c>GETRANGE</c>.</summary>
    public ValueTask<string> GetSubstringAsync(string key, long start, long end, CancellationToken cancellationToken = default)
        => RespRead.RequiredText(ExecuteCommandAsync(CoverageCommands.GetRange("SUBSTR", key, start, end), cancellationToken), "SUBSTR");

    /// <summary>Sends <c>SETRANGE</c> and returns the new string length.</summary>
    public ValueTask<long> SetRangeAsync(string key, long offset, string value, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.SetRange(key, offset, value), cancellationToken), "SETRANGE");

    /// <summary>Sends <c>MSETNX</c>. True when every key was missing and all were written; false writes nothing.</summary>
    public ValueTask<bool> SetManyIfNotExistsAsync(params (string Key, string Value)[] pairs)
        => RespRead.Flag(ExecuteCommandAsync(CoverageCommands.SetManyIfNotExists(pairs), CancellationToken.None), "MSETNX");

    /// <summary>Sends <c>SETBIT</c> and returns the previous bit.</summary>
    public ValueTask<bool> SetBitAsync(string key, long offset, bool value, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteCommandAsync(CoverageCommands.SetBit(key, offset, value), cancellationToken), "SETBIT");

    /// <summary>Sends <c>GETBIT</c>.</summary>
    public ValueTask<bool> GetBitAsync(string key, long offset, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteCommandAsync(CoverageCommands.GetBit(key, offset), cancellationToken), "GETBIT");

    /// <summary>Sends <c>BITCOUNT key [start end [BYTE|BIT]]</c>.</summary>
    public ValueTask<long> BitCountAsync(string key, long? start = null, long? end = null, BitUnit unit = BitUnit.Byte, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.BitCount(key, start, end, unit), cancellationToken), "BITCOUNT");

    /// <summary>Sends <c>BITPOS key bit [start [end [BYTE|BIT]]]</c>. -1 when the bit is not found.</summary>
    public ValueTask<long> BitPositionAsync(string key, bool bit, long? start = null, long? end = null, BitUnit unit = BitUnit.Byte, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.BitPosition(key, bit, start, end, unit), cancellationToken), "BITPOS");

    /// <summary>Sends <c>BITOP operation destination key...</c> and returns the destination length.</summary>
    public ValueTask<long> BitOperationAsync(BitOperation operation, string destination, IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.BitOperation(operation, destination, keys), cancellationToken), "BITOP");

    /// <summary>Sends <c>PFADD</c>. True when the estimate changed.</summary>
    public ValueTask<bool> HyperLogLogAddAsync(string key, params string[] elements)
        => RespRead.Flag(ExecuteCommandAsync(CoverageCommands.Members("PFADD", key, elements), CancellationToken.None), "PFADD");

    /// <summary>Sends <c>PFCOUNT</c>: the estimate of one key, or of the union of several.</summary>
    public ValueTask<long> HyperLogLogCountAsync(params string[] keys)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.Keys("PFCOUNT", keys), CancellationToken.None), "PFCOUNT");

    /// <summary>Sends <c>PFMERGE destination source...</c>.</summary>
    public ValueTask HyperLogLogMergeAsync(string destination, params string[] sources)
        => RespRead.Ok(ExecuteCommandAsync(CoverageCommands.Members("PFMERGE", destination, sources), CancellationToken.None));

    /// <summary>Sends <c>LPUSHCAP key cap value...</c> (Ruvio extension): pushes like <c>LPUSH</c>, then trims the tail to at most <paramref name="cap"/> items. Returns the new length.</summary>
    public ValueTask<long> PushListLeftCappedAsync(string key, long cap, params string[] values)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.Capped("LPUSHCAP", key, cap, values), CancellationToken.None), "LPUSHCAP");

    /// <summary>Sends <c>RPUSHCAP key cap value...</c> (Ruvio extension): pushes like <c>RPUSH</c>, then trims the head to at most <paramref name="cap"/> items. Returns the new length.</summary>
    public ValueTask<long> PushListRightCappedAsync(string key, long cap, params string[] values)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.Capped("RPUSHCAP", key, cap, values), CancellationToken.None), "RPUSHCAP");

    /// <summary>Sends <c>BLMPOP</c>. Null when <paramref name="timeout"/> passes (rounded up to seconds; zero waits forever). The shared connection sends nothing else while it waits.</summary>
    public ValueTask<KeyValuePair<string, IReadOnlyList<string>>?> PopListManyBlockingAsync(IReadOnlyList<string> keys, bool fromLeft, TimeSpan timeout, long count = 1, CancellationToken cancellationToken = default)
        => RespRead.ListPop(ExecuteCommandAsync(CoverageCommands.PopListManyBlocking(keys, fromLeft, timeout, count), cancellationToken), "BLMPOP");

    /// <summary>Sends <c>SMISMEMBER</c>: one membership flag per member.</summary>
    public ValueTask<IReadOnlyList<bool>> SetContainsManyAsync(string key, params string[] members)
        => RespRead.Flags(ExecuteCommandAsync(CoverageCommands.Members("SMISMEMBER", key, members), CancellationToken.None), "SMISMEMBER");

    /// <summary>Sends <c>SINTERCARD numkeys key... [LIMIT n]</c>. <paramref name="limit"/> 0 counts everything.</summary>
    public ValueTask<long> GetSetIntersectionCountAsync(IReadOnlyList<string> keys, long limit = 0, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.SetIntersectionCount(keys, limit), cancellationToken), "SINTERCARD");

    /// <summary>Sends <c>SSCAN key cursor [MATCH pattern] [COUNT n]</c>. See <c>ScanSetMembersAsync</c> for a lazy iterator.</summary>
    public ValueTask<SetScanPage> ScanSetAsync(string key, long cursor, string? pattern = null, int? count = null, CancellationToken cancellationToken = default)
        => RespRead.SetScan(ExecuteCommandAsync(CoverageCommands.KeyScan("SSCAN", key, cursor, pattern, count), cancellationToken));

    /// <summary>Sends <c>SCAN cursor [MATCH pattern] [COUNT n] [TYPE type]</c>.</summary>
    public ValueTask<ScanPage> ScanAsync(long cursor, string? pattern, int? count, string? type, CancellationToken cancellationToken = default)
        => ScanPageOf(ExecuteCommandAsync(CoverageCommands.Scan(cursor, pattern, count, type), cancellationToken));

    /// <summary>Sends <c>ZRANGEBYLEX</c>, or <c>ZREVRANGEBYLEX key max min</c> when <paramref name="descending"/>. Bounds are <c>-</c>, <c>+</c>, <c>[member</c>, <c>(member</c>; a negative <paramref name="count"/> means no <c>LIMIT</c>.</summary>
    public ValueTask<IReadOnlyList<string>> GetSortedSetRangeByLexAsync(string key, string min, string max, bool descending, long offset = 0, long count = -1, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteCommandAsync(CoverageCommands.RangeByLex(key, min, max, descending, offset, count), cancellationToken), "ZRANGEBYLEX");

    /// <summary>Sends <c>ZRANGE key min max BYSCORE [REV] [LIMIT offset count]</c> without scores. Bounds accept <c>-inf</c>, <c>+inf</c> and <c>(</c>.</summary>
    public ValueTask<IReadOnlyList<string>> GetSortedSetRangeByScoreAsync(string key, string min, string max, bool descending = false, long offset = 0, long count = -1, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteCommandAsync(CoverageCommands.RangeByScore(key, min, max, descending, offset, count), cancellationToken), "ZRANGE");

    /// <summary>Sends <c>ZCOUNT key min max</c>.</summary>
    public ValueTask<long> CountSortedSetByScoreAsync(string key, string min, string max, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(new RespCommand("ZCOUNT", key, min, max), cancellationToken), "ZCOUNT");

    /// <summary>Sends <c>ZLEXCOUNT key min max</c>.</summary>
    public ValueTask<long> CountSortedSetByLexAsync(string key, string min, string max, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(new RespCommand("ZLEXCOUNT", key, min, max), cancellationToken), "ZLEXCOUNT");

    /// <summary>Sends <c>ZMSCORE</c>: one score per member, null when missing.</summary>
    public ValueTask<IReadOnlyList<double?>> GetSortedSetScoresAsync(string key, params string[] members)
        => RespRead.Scores(ExecuteCommandAsync(CoverageCommands.Members("ZMSCORE", key, members), CancellationToken.None), "ZMSCORE");

    /// <summary>Sends <c>ZRANDMEMBER key</c>. Null for a missing key.</summary>
    public ValueTask<string?> GetSortedSetRandomMemberAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteCommandAsync(new RespCommand("ZRANDMEMBER", key), cancellationToken));

    /// <summary>Sends <c>ZRANDMEMBER key count</c>. A negative count may repeat members.</summary>
    public ValueTask<IReadOnlyList<string>> GetSortedSetRandomMembersAsync(string key, long count, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteCommandAsync(CoverageCommands.RandomMembers("ZRANDMEMBER", key, count, null), cancellationToken), "ZRANDMEMBER");

    /// <summary>Sends <c>ZRANDMEMBER key count WITHSCORES</c>.</summary>
    public ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetRandomMembersWithScoresAsync(string key, long count, CancellationToken cancellationToken = default)
        => RespRead.Entries(ExecuteCommandAsync(CoverageCommands.RandomMembers("ZRANDMEMBER", key, count, "WITHSCORES"), cancellationToken), "ZRANDMEMBER");

    /// <summary>Sends <c>ZSCAN key cursor [MATCH pattern] [COUNT n]</c>. See <c>ScanSortedSetEntriesAsync</c> for a lazy iterator.</summary>
    public ValueTask<SortedSetScanPage> ScanSortedSetAsync(string key, long cursor, string? pattern = null, int? count = null, CancellationToken cancellationToken = default)
        => RespRead.SortedSetScan(ExecuteCommandAsync(CoverageCommands.KeyScan("ZSCAN", key, cursor, pattern, count), cancellationToken));

    /// <summary>Sends <c>ZUNION numkeys key... [WEIGHTS] [AGGREGATE] WITHSCORES</c>.</summary>
    public ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetUnionAsync(IReadOnlyList<string> keys, IReadOnlyList<double>? weights = null, SortedSetAggregate? aggregate = null, CancellationToken cancellationToken = default)
        => RespRead.Entries(ExecuteCommandAsync(CoverageCommands.Combine("ZUNION", null, keys, weights, aggregate, true), cancellationToken), "ZUNION");

    /// <summary>Sends <c>ZINTER numkeys key... [WEIGHTS] [AGGREGATE] WITHSCORES</c>.</summary>
    public ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetIntersectionAsync(IReadOnlyList<string> keys, IReadOnlyList<double>? weights = null, SortedSetAggregate? aggregate = null, CancellationToken cancellationToken = default)
        => RespRead.Entries(ExecuteCommandAsync(CoverageCommands.Combine("ZINTER", null, keys, weights, aggregate, true), cancellationToken), "ZINTER");

    /// <summary>Sends <c>ZDIFF numkeys key... WITHSCORES</c>: members of the first set missing from the others.</summary>
    public ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetDifferenceAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
        => RespRead.Entries(ExecuteCommandAsync(CoverageCommands.Combine("ZDIFF", null, keys, null, null, true), cancellationToken), "ZDIFF");

    /// <summary>Sends <c>ZUNIONSTORE</c> and returns the destination size.</summary>
    public ValueTask<long> StoreSortedSetUnionAsync(string destination, IReadOnlyList<string> keys, IReadOnlyList<double>? weights = null, SortedSetAggregate? aggregate = null, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.Combine("ZUNIONSTORE", destination, keys, weights, aggregate, false), cancellationToken), "ZUNIONSTORE");

    /// <summary>Sends <c>ZINTERSTORE</c> and returns the destination size.</summary>
    public ValueTask<long> StoreSortedSetIntersectionAsync(string destination, IReadOnlyList<string> keys, IReadOnlyList<double>? weights = null, SortedSetAggregate? aggregate = null, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.Combine("ZINTERSTORE", destination, keys, weights, aggregate, false), cancellationToken), "ZINTERSTORE");

    /// <summary>Sends <c>ZDIFFSTORE</c> and returns the destination size.</summary>
    public ValueTask<long> StoreSortedSetDifferenceAsync(string destination, IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.Combine("ZDIFFSTORE", destination, keys, null, null, false), cancellationToken), "ZDIFFSTORE");

    /// <summary>Sends <c>HRANDFIELD key</c>. Null for a missing key.</summary>
    public ValueTask<string?> GetHashRandomFieldAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteCommandAsync(new RespCommand("HRANDFIELD", key), cancellationToken));

    /// <summary>Sends <c>HRANDFIELD key count</c>. A negative count may repeat fields.</summary>
    public ValueTask<IReadOnlyList<string>> GetHashRandomFieldsAsync(string key, long count, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteCommandAsync(CoverageCommands.RandomMembers("HRANDFIELD", key, count, null), cancellationToken), "HRANDFIELD");

    /// <summary>Sends <c>HRANDFIELD key count WITHVALUES</c>.</summary>
    public ValueTask<IReadOnlyList<KeyValuePair<string, string>>> GetHashRandomFieldsWithValuesAsync(string key, long count, CancellationToken cancellationToken = default)
        => RespRead.Pairs(ExecuteCommandAsync(CoverageCommands.RandomMembers("HRANDFIELD", key, count, "WITHVALUES"), cancellationToken), "HRANDFIELD");

    /// <summary>Sends <c>HINCRBYFLOAT</c> and returns the new value.</summary>
    public ValueTask<double> IncrementHashFieldByFloatAsync(string key, string field, double delta, CancellationToken cancellationToken = default)
        => RespRead.Double(ExecuteCommandAsync(CoverageCommands.IncrementHashFieldByFloat(key, field, delta), cancellationToken), "HINCRBYFLOAT");

    /// <summary>Sends <c>BF.RESERVE key error capacity [EXPANSION n] [NONSCALING]</c>.</summary>
    public ValueTask ReserveBloomFilterAsync(string key, double errorRate, long capacity, long? expansion, bool nonScaling = false, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteCommandAsync(CoverageCommands.ReserveBloom(key, errorRate, capacity, expansion, nonScaling), cancellationToken));

    /// <summary>Sends <c>BF.MADD</c>: one flag per item, true when it was added (not seen before). A full non-scaling filter throws.</summary>
    public ValueTask<IReadOnlyList<bool>> AddBloomFilterManyAsync(string key, params string[] items)
        => RespRead.Flags(ExecuteCommandAsync(CoverageCommands.Members("BF.MADD", key, items), CancellationToken.None), "BF.MADD");

    /// <summary>Sends <c>BF.MEXISTS</c>: one flag per item, true when it may have been added.</summary>
    public ValueTask<IReadOnlyList<bool>> BloomFilterContainsManyAsync(string key, params string[] items)
        => RespRead.Flags(ExecuteCommandAsync(CoverageCommands.Members("BF.MEXISTS", key, items), CancellationToken.None), "BF.MEXISTS");

    /// <summary>Sends <c>BF.INSERT key [CAPACITY] [ERROR] [EXPANSION] [NOCREATE] [NONSCALING] ITEMS item...</c>: one flag per item, true when added.</summary>
    public ValueTask<IReadOnlyList<bool>> InsertBloomFilterAsync(string key, IReadOnlyList<string> items, BloomInsertOptions? options = null, CancellationToken cancellationToken = default)
        => RespRead.Flags(ExecuteCommandAsync(CoverageCommands.InsertBloom(key, items, options), cancellationToken), "BF.INSERT");

    /// <summary>Sends <c>BF.INFO key</c>.</summary>
    public ValueTask<BloomFilterInfo> GetBloomFilterInfoAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.BloomInfo(ExecuteCommandAsync(new RespCommand("BF.INFO", key), cancellationToken));

    /// <summary>Sends <c>BF.CARD</c>: items added so far; 0 for a missing key.</summary>
    public ValueTask<long> GetBloomFilterCardinalityAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(new RespCommand("BF.CARD", key), cancellationToken), "BF.CARD");

    /// <summary>Sends <c>JSON.SET key path json NX|XX</c>. False when the condition or the path skipped the write.</summary>
    public ValueTask<bool> JsonSetAsync(string key, string path, string json, SetMode mode, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteCommandAsync(CoverageCommands.JsonSet(key, path, json, mode), cancellationToken));

    /// <summary>Sends <c>JSON.OBJLEN</c>. Null when the key or path is absent.</summary>
    public ValueTask<long?> JsonObjectLengthAsync(string key, string path = "$", CancellationToken cancellationToken = default)
        => RespRead.OptionalInteger(ExecuteCommandAsync(new RespCommand("JSON.OBJLEN", key, path), cancellationToken), "JSON.OBJLEN");

    /// <summary>Sends <c>JSON.STRAPPEND key path json-string</c>; <paramref name="json"/> is a JSON string literal such as <c>"\"abc\""</c>. Returns the new length in bytes.</summary>
    public ValueTask<long?> JsonStringAppendAsync(string key, string path, string json, CancellationToken cancellationToken = default)
        => RespRead.OptionalInteger(ExecuteCommandAsync(new RespCommand("JSON.STRAPPEND", key, path, json), cancellationToken), "JSON.STRAPPEND");

    /// <summary>Sends <c>JSON.NUMMULTBY</c> and returns the new number as JSON text; null when absent.</summary>
    public ValueTask<string?> JsonMultiplyAsync(string key, string path, double by, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteCommandAsync(new RespCommand("JSON.NUMMULTBY", key, path, RespRead.Number(by)), cancellationToken));

    /// <summary>Sends <c>JSON.ARRINSERT key path index json...</c> and returns the new length.</summary>
    public ValueTask<long?> JsonArrayInsertAsync(string key, string path, long index, params string[] json)
        => RespRead.OptionalInteger(ExecuteCommandAsync(CoverageCommands.JsonArrayInsert(key, path, index, json), CancellationToken.None), "JSON.ARRINSERT");

    /// <summary>Sends <c>JSON.ARRPOP key path index</c> and returns the removed element as JSON; null for an empty array.</summary>
    public ValueTask<string?> JsonArrayPopAsync(string key, string path = "$", long index = -1, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteCommandAsync(new RespCommand("JSON.ARRPOP", key, path, index), cancellationToken));

    /// <summary>Sends <c>JSON.ARRTRIM key path start stop</c> (inclusive) and returns the new length.</summary>
    public ValueTask<long?> JsonArrayTrimAsync(string key, string path, long start, long stop, CancellationToken cancellationToken = default)
        => RespRead.OptionalInteger(ExecuteCommandAsync(new RespCommand("JSON.ARRTRIM", key, path, start, stop), cancellationToken), "JSON.ARRTRIM");

    /// <summary>Sends <c>JSON.ARRINDEX key path scalar [start stop]</c>; <paramref name="scalar"/> is JSON text. -1 when absent; a stop of 0 means the end.</summary>
    public ValueTask<long?> JsonArrayIndexAsync(string key, string path, string scalar, long start = 0, long stop = 0, CancellationToken cancellationToken = default)
        => RespRead.OptionalInteger(ExecuteCommandAsync(CoverageCommands.JsonArrayIndex(key, path, scalar, start, stop), cancellationToken), "JSON.ARRINDEX");

    /// <summary>Sends <c>JSON.FORGET</c>, the alias of <c>JSON.DEL</c>.</summary>
    public ValueTask<long> JsonForgetAsync(string key, string path = "$", CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(new RespCommand("JSON.FORGET", key, path), cancellationToken), "JSON.FORGET");

    /// <summary>Sends <c>JMERGE</c>, the alias of <c>JSON.MERGE</c>. Prefer <c>JsonMergeAsync</c>.</summary>
    public ValueTask<bool> JMergeAsync(string key, string path, string patch, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteCommandAsync(new RespCommand("JMERGE", key, path, patch), cancellationToken));

    /// <summary>Sends <c>JSON.INDEX CREATE name PREFIX prefix PATH path TYPE type</c> (Ruvio extension; database 0 only). The index builds in the background; <c>JSON.FIND</c> answers <c>ERR index is building</c> until it is ready.</summary>
    public ValueTask JsonCreateIndexAsync(string name, string prefix, string path, JsonIndexType type = JsonIndexType.String, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteCommandAsync(CoverageCommands.JsonCreateIndex(name, prefix, path, type), cancellationToken));

    /// <summary>Sends <c>JSON.INDEX DROP name</c>. False when the index did not exist.</summary>
    public ValueTask<bool> JsonDropIndexAsync(string name, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteCommandAsync(new RespCommand("JSON.INDEX", "DROP", name), cancellationToken), "JSON.INDEX");

    /// <summary>Sends <c>JSON.INDEX LIST</c>: one description line per index (name, prefix, path, type, state, keys, memory).</summary>
    public ValueTask<IReadOnlyList<string>> JsonListIndexesAsync(CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteCommandAsync(new RespCommand("JSON.INDEX", "LIST"), cancellationToken), "JSON.INDEX");

    /// <summary>Sends <c>JSON.FIND index VALUE value [COUNT n] [AFTER key]</c> (Ruvio extension): matching keys in ascending order. Page with <paramref name="after"/> set to the last key.</summary>
    public ValueTask<IReadOnlyList<string>> JsonFindAsync(string index, string value, int? count = null, string? after = null, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteCommandAsync(CoverageCommands.JsonFind(index, value, count, after), cancellationToken), "JSON.FIND");

    /// <summary>Sends <c>RUVIO</c> and returns the server's answer.</summary>
    public ValueTask<string> RuvioAsync(CancellationToken cancellationToken = default)
        => RespRead.RequiredText(ExecuteCommandAsync(new RespCommand("RUVIO"), cancellationToken), "RUVIO");

    /// <summary>Sends <c>RUVIO OWNER</c>: names the shard rule.</summary>
    public ValueTask<string> GetRuvioOwnerAsync(CancellationToken cancellationToken = default)
        => RespRead.RequiredText(ExecuteCommandAsync(new RespCommand("RUVIO", "OWNER"), cancellationToken), "RUVIO");

    /// <summary>Sends <c>COMMAND</c>: the whole command table.</summary>
    public ValueTask<RespValue> GetCommandsAsync(CancellationToken cancellationToken = default)
        => ExecuteCommandAsync(new RespCommand("COMMAND"), cancellationToken);

    /// <summary>Sends <c>COMMAND COUNT</c>.</summary>
    public ValueTask<long> GetCommandCountAsync(CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(new RespCommand("COMMAND", "COUNT"), cancellationToken), "COMMAND");

    /// <summary>Sends <c>COMMAND INFO name...</c> (Redis 7 shape).</summary>
    public ValueTask<RespValue> GetCommandInfoAsync(params string[] names)
        => ExecuteCommandAsync(CoverageCommands.Words("COMMAND", "INFO", names), CancellationToken.None);

    /// <summary>Sends <c>COMMAND DOCS name...</c>.</summary>
    public ValueTask<RespValue> GetCommandDocsAsync(params string[] names)
        => ExecuteCommandAsync(CoverageCommands.Words("COMMAND", "DOCS", names), CancellationToken.None);

    /// <summary>Sends <c>LATENCY LATEST</c>. Ruvio records no spike events, so this is empty.</summary>
    public ValueTask<IReadOnlyList<RespValue>> GetLatencyLatestAsync(CancellationToken cancellationToken = default)
        => ItemsOf(ExecuteCommandAsync(new RespCommand("LATENCY", "LATEST"), cancellationToken));

    /// <summary>Sends <c>WAIT replicas timeout</c> and returns how many replicas reached this shard's last write. Zero timeout waits until shutdown. The shared connection sends nothing else while it waits.</summary>
    public ValueTask<long> WaitForReplicasAsync(long replicas, TimeSpan timeout, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.Wait(replicas, timeout), cancellationToken), "WAIT");

    /// <summary>Sends <c>AUTH password</c> on the shared connection. Every caller of this client then runs as that user; a reconnect authenticates with <see cref="RuvioClientOptions"/> again.</summary>
    public ValueTask AuthenticateAsync(string password, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteCommandAsync(new RespCommand("AUTH", password), cancellationToken));

    /// <summary>Sends <c>AUTH username password</c> on the shared connection. Every caller of this client then runs as that user; a reconnect authenticates with <see cref="RuvioClientOptions"/> again.</summary>
    public ValueTask AuthenticateAsync(string username, string password, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteCommandAsync(new RespCommand("AUTH", username, password), cancellationToken));

    /// <summary>Sends <c>CLIENT KILL ADDR ip:port</c> and returns how many connections were closed.</summary>
    public ValueTask<long> KillClientAsync(string address, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(new RespCommand("CLIENT", "KILL", "ADDR", address), cancellationToken), "CLIENT");

    /// <summary>Sends <c>IDEM key BEGIN fingerprint owner ttl</c> and reads the decision.</summary>
    public ValueTask<IdempotencyBegin> TryBeginIdempotencyAsync(string key, string fingerprint, string owner, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Idempotency(ExecuteCommandAsync(new RespCommand("IDEM", key, "BEGIN", fingerprint, owner, RespRead.Milliseconds(ttl)), cancellationToken));
}
