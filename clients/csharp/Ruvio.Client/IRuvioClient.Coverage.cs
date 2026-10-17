// Default members for the coverage pass. An IRuvioClient implementation that only implements
// ExecuteAsync(IReadOnlyList<string>, ...) still gets every command below; RuvioClient overrides each one
// with the inline (allocation-free argument) path.

namespace Ruvio.Client;

public partial interface IRuvioClient
{
    /// <summary>Sends <c>SET key value IFEQ expected [PX ms]</c> (Ruvio extension). True when the stored string equaled <paramref name="expected"/> and was replaced; false (nothing written) otherwise.</summary>
    ValueTask<bool> SetIfEqualAsync(string key, string value, string expected, TimeSpan? expiry = null, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteAsync(CoverageCommands.SetIfEqual(key, value, expected, expiry).AsList(), cancellationToken));

    /// <summary>Sends <c>SET key value IFEQ expected KEEPTTL</c>: compare-and-set that keeps the current expiry.</summary>
    ValueTask<bool> SetIfEqualKeepingTimeToLiveAsync(string key, string value, string expected, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteAsync(CoverageCommands.SetIfEqualKeepingTimeToLive(key, value, expected).AsList(), cancellationToken));

    /// <summary>Sends <c>DELEX key IFEQ expected</c> (Ruvio extension). True when the string equaled <paramref name="expected"/> and was deleted.</summary>
    ValueTask<bool> DeleteIfEqualAsync(string key, string expected, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteAsync(CoverageCommands.DeleteIfEqual(key, expected).AsList(), cancellationToken), "DELEX");

    /// <summary>Sends <c>SETEX key seconds value</c>.</summary>
    ValueTask SetExpireAsync(string key, long seconds, string value, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteAsync(CoverageCommands.SetExpire(key, seconds, value).AsList(), cancellationToken));

    /// <summary>Sends <c>PSETEX key milliseconds value</c>.</summary>
    ValueTask SetExpireMillisecondsAsync(string key, long milliseconds, string value, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteAsync(CoverageCommands.SetExpireMilliseconds(key, milliseconds, value).AsList(), cancellationToken));

    /// <summary>Sends <c>SETNX</c>. True when the key was missing and was written.</summary>
    ValueTask<bool> SetIfNotExistsAsync(string key, string value, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteAsync(CoverageCommands.SetIfNotExists(key, value).AsList(), cancellationToken), "SETNX");

    /// <summary>Sends <c>GETDEL</c>: returns the string and deletes the key. Null when missing.</summary>
    ValueTask<string?> GetDeleteAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteAsync(CoverageCommands.GetDelete(key).AsList(), cancellationToken));

    /// <summary>Sends <c>INCRBYFLOAT</c> and returns the new value.</summary>
    ValueTask<double> IncrementByFloatAsync(string key, double delta, CancellationToken cancellationToken = default)
        => RespRead.Double(ExecuteAsync(CoverageCommands.IncrementByFloat(key, delta).AsList(), cancellationToken), "INCRBYFLOAT");

    /// <summary>Sends <c>GETRANGE key start end</c> (inclusive, negative from the end).</summary>
    ValueTask<string> GetRangeAsync(string key, long start, long end, CancellationToken cancellationToken = default)
        => RespRead.RequiredText(ExecuteAsync(CoverageCommands.GetRange("GETRANGE", key, start, end).AsList(), cancellationToken), "GETRANGE");

    /// <summary>Sends <c>SUBSTR</c>, the legacy alias of <c>GETRANGE</c>.</summary>
    ValueTask<string> GetSubstringAsync(string key, long start, long end, CancellationToken cancellationToken = default)
        => RespRead.RequiredText(ExecuteAsync(CoverageCommands.GetRange("SUBSTR", key, start, end).AsList(), cancellationToken), "SUBSTR");

    /// <summary>Sends <c>SETRANGE</c> and returns the new string length.</summary>
    ValueTask<long> SetRangeAsync(string key, long offset, string value, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.SetRange(key, offset, value).AsList(), cancellationToken), "SETRANGE");

    /// <summary>Sends <c>MSETNX</c>. True when every key was missing and all were written; false writes nothing.</summary>
    ValueTask<bool> SetManyIfNotExistsAsync(params (string Key, string Value)[] pairs)
        => RespRead.Flag(ExecuteAsync(CoverageCommands.SetManyIfNotExists(pairs).AsList(), CancellationToken.None), "MSETNX");

    /// <summary>Sends <c>SETBIT</c> and returns the previous bit.</summary>
    ValueTask<bool> SetBitAsync(string key, long offset, bool value, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteAsync(CoverageCommands.SetBit(key, offset, value).AsList(), cancellationToken), "SETBIT");

    /// <summary>Sends <c>GETBIT</c>.</summary>
    ValueTask<bool> GetBitAsync(string key, long offset, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteAsync(CoverageCommands.GetBit(key, offset).AsList(), cancellationToken), "GETBIT");

    /// <summary>Sends <c>BITCOUNT key [start end [BYTE|BIT]]</c>.</summary>
    ValueTask<long> BitCountAsync(string key, long? start = null, long? end = null, BitUnit unit = BitUnit.Byte, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.BitCount(key, start, end, unit).AsList(), cancellationToken), "BITCOUNT");

    /// <summary>Sends <c>BITPOS key bit [start [end [BYTE|BIT]]]</c>. -1 when the bit is not found.</summary>
    ValueTask<long> BitPositionAsync(string key, bool bit, long? start = null, long? end = null, BitUnit unit = BitUnit.Byte, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.BitPosition(key, bit, start, end, unit).AsList(), cancellationToken), "BITPOS");

    /// <summary>Sends <c>BITOP operation destination key...</c> and returns the destination length.</summary>
    ValueTask<long> BitOperationAsync(BitOperation operation, string destination, IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.BitOperation(operation, destination, keys).AsList(), cancellationToken), "BITOP");

    /// <summary>Sends <c>PFADD</c>. True when the estimate changed.</summary>
    ValueTask<bool> HyperLogLogAddAsync(string key, params string[] elements)
        => RespRead.Flag(ExecuteAsync(CoverageCommands.Members("PFADD", key, elements).AsList(), CancellationToken.None), "PFADD");

    /// <summary>Sends <c>PFCOUNT</c>: the estimate of one key, or of the union of several.</summary>
    ValueTask<long> HyperLogLogCountAsync(params string[] keys)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.Keys("PFCOUNT", keys).AsList(), CancellationToken.None), "PFCOUNT");

    /// <summary>Sends <c>PFMERGE destination source...</c>.</summary>
    ValueTask HyperLogLogMergeAsync(string destination, params string[] sources)
        => RespRead.Ok(ExecuteAsync(CoverageCommands.Members("PFMERGE", destination, sources).AsList(), CancellationToken.None));

    /// <summary>Sends <c>LPUSHCAP key cap value...</c> (Ruvio extension): pushes like <c>LPUSH</c>, then trims the tail to at most <paramref name="cap"/> items. Returns the new length.</summary>
    ValueTask<long> PushListLeftCappedAsync(string key, long cap, params string[] values)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.Capped("LPUSHCAP", key, cap, values).AsList(), CancellationToken.None), "LPUSHCAP");

    /// <summary>Sends <c>RPUSHCAP key cap value...</c> (Ruvio extension): pushes like <c>RPUSH</c>, then trims the head to at most <paramref name="cap"/> items. Returns the new length.</summary>
    ValueTask<long> PushListRightCappedAsync(string key, long cap, params string[] values)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.Capped("RPUSHCAP", key, cap, values).AsList(), CancellationToken.None), "RPUSHCAP");

    /// <summary>Sends <c>BLMPOP</c>. Null when <paramref name="timeout"/> passes (rounded up to seconds; zero waits forever). The shared connection sends nothing else while it waits.</summary>
    ValueTask<KeyValuePair<string, IReadOnlyList<string>>?> PopListManyBlockingAsync(IReadOnlyList<string> keys, bool fromLeft, TimeSpan timeout, long count = 1, CancellationToken cancellationToken = default)
        => RespRead.ListPop(ExecuteAsync(CoverageCommands.PopListManyBlocking(keys, fromLeft, timeout, count).AsList(), cancellationToken), "BLMPOP");

    /// <summary>Sends <c>BLMPOP</c> with a typed side. Null when <paramref name="timeout"/> passes.</summary>
    ValueTask<KeyValuePair<string, IReadOnlyList<string>>?> PopListManyBlockingAsync(IReadOnlyList<string> keys, ListSide side, TimeSpan timeout, long count = 1, CancellationToken cancellationToken = default)
        => PopListManyBlockingAsync(keys, side == ListSide.Left, timeout, count, cancellationToken);

    /// <summary>Sends <c>SMISMEMBER</c>: one membership flag per member.</summary>
    ValueTask<IReadOnlyList<bool>> SetContainsManyAsync(string key, params string[] members)
        => RespRead.Flags(ExecuteAsync(CoverageCommands.Members("SMISMEMBER", key, members).AsList(), CancellationToken.None), "SMISMEMBER");

    /// <summary>Sends <c>SINTERCARD numkeys key... [LIMIT n]</c>. <paramref name="limit"/> 0 counts everything.</summary>
    ValueTask<long> GetSetIntersectionCountAsync(IReadOnlyList<string> keys, long limit = 0, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.SetIntersectionCount(keys, limit).AsList(), cancellationToken), "SINTERCARD");

    /// <summary>Sends <c>SSCAN key cursor [MATCH pattern] [COUNT n]</c>. See <c>ScanSetMembersAsync</c> for a lazy iterator.</summary>
    ValueTask<SetScanPage> ScanSetAsync(string key, long cursor, string? pattern = null, int? count = null, CancellationToken cancellationToken = default)
        => RespRead.SetScan(ExecuteAsync(CoverageCommands.KeyScan("SSCAN", key, cursor, pattern, count).AsList(), cancellationToken));

    /// <summary>Sends <c>SCAN cursor [MATCH pattern] [COUNT n] [TYPE type]</c>.</summary>
    ValueTask<ScanPage> ScanAsync(long cursor, string? pattern, int? count, string? type, CancellationToken cancellationToken = default)
        => RuvioClient.ScanPageOf(ExecuteAsync(CoverageCommands.Scan(cursor, pattern, count, type).AsList(), cancellationToken));

    /// <summary>Sends <c>SCAN cursor [MATCH pattern] [COUNT n] TYPE type</c> with a typed value type. Example: <c>ScanAsync(0, "user:*", 100, KeyType.Hash)</c>.</summary>
    ValueTask<ScanPage> ScanAsync(long cursor, string? pattern, int? count, KeyType type, CancellationToken cancellationToken = default)
        => ScanAsync(cursor, pattern, count, Keywords.Word(type), cancellationToken);

    /// <summary>Sends <c>ZRANGEBYLEX</c>, or <c>ZREVRANGEBYLEX key max min</c> when <paramref name="descending"/>. Bounds are <c>-</c>, <c>+</c>, <c>[member</c>, <c>(member</c>; a negative <paramref name="count"/> means no <c>LIMIT</c>.</summary>
    ValueTask<IReadOnlyList<string>> GetSortedSetRangeByLexAsync(string key, string min, string max, bool descending, long offset = 0, long count = -1, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteAsync(CoverageCommands.RangeByLex(key, min, max, descending, offset, count).AsList(), cancellationToken), "ZRANGEBYLEX");

    /// <summary>Sends <c>ZRANGE key min max BYSCORE [REV] [LIMIT offset count]</c> without scores. Bounds accept <c>-inf</c>, <c>+inf</c> and <c>(</c>.</summary>
    ValueTask<IReadOnlyList<string>> GetSortedSetRangeByScoreAsync(string key, string min, string max, bool descending = false, long offset = 0, long count = -1, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteAsync(CoverageCommands.RangeByScore(key, min, max, descending, offset, count).AsList(), cancellationToken), "ZRANGE");

    /// <summary>Sends <c>ZCOUNT key min max</c>.</summary>
    ValueTask<long> CountSortedSetByScoreAsync(string key, string min, string max, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(new RespCommand("ZCOUNT", key, min, max).AsList(), cancellationToken), "ZCOUNT");

    /// <summary>Sends <c>ZLEXCOUNT key min max</c>.</summary>
    ValueTask<long> CountSortedSetByLexAsync(string key, string min, string max, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(new RespCommand("ZLEXCOUNT", key, min, max).AsList(), cancellationToken), "ZLEXCOUNT");

    /// <summary>Sends <c>ZMSCORE</c>: one score per member, null when missing.</summary>
    ValueTask<IReadOnlyList<double?>> GetSortedSetScoresAsync(string key, params string[] members)
        => RespRead.Scores(ExecuteAsync(CoverageCommands.Members("ZMSCORE", key, members).AsList(), CancellationToken.None), "ZMSCORE");

    /// <summary>Sends <c>ZRANDMEMBER key</c>. Null for a missing key.</summary>
    ValueTask<string?> GetSortedSetRandomMemberAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteAsync(new RespCommand("ZRANDMEMBER", key).AsList(), cancellationToken));

    /// <summary>Sends <c>ZRANDMEMBER key count</c>. A negative count may repeat members.</summary>
    ValueTask<IReadOnlyList<string>> GetSortedSetRandomMembersAsync(string key, long count, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteAsync(CoverageCommands.RandomMembers("ZRANDMEMBER", key, count, null).AsList(), cancellationToken), "ZRANDMEMBER");

    /// <summary>Sends <c>ZRANDMEMBER key count WITHSCORES</c>.</summary>
    ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetRandomMembersWithScoresAsync(string key, long count, CancellationToken cancellationToken = default)
        => RespRead.Entries(ExecuteAsync(CoverageCommands.RandomMembers("ZRANDMEMBER", key, count, "WITHSCORES").AsList(), cancellationToken), "ZRANDMEMBER");

    /// <summary>Sends <c>ZSCAN key cursor [MATCH pattern] [COUNT n]</c>. See <c>ScanSortedSetEntriesAsync</c> for a lazy iterator.</summary>
    ValueTask<SortedSetScanPage> ScanSortedSetAsync(string key, long cursor, string? pattern = null, int? count = null, CancellationToken cancellationToken = default)
        => RespRead.SortedSetScan(ExecuteAsync(CoverageCommands.KeyScan("ZSCAN", key, cursor, pattern, count).AsList(), cancellationToken));

    /// <summary>Sends <c>ZUNION numkeys key... [WEIGHTS] [AGGREGATE] WITHSCORES</c>.</summary>
    ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetUnionAsync(IReadOnlyList<string> keys, IReadOnlyList<double>? weights = null, SortedSetAggregate? aggregate = null, CancellationToken cancellationToken = default)
        => RespRead.Entries(ExecuteAsync(CoverageCommands.Combine("ZUNION", null, keys, weights, aggregate, true).AsList(), cancellationToken), "ZUNION");

    /// <summary>Sends <c>ZINTER numkeys key... [WEIGHTS] [AGGREGATE] WITHSCORES</c>.</summary>
    ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetIntersectionAsync(IReadOnlyList<string> keys, IReadOnlyList<double>? weights = null, SortedSetAggregate? aggregate = null, CancellationToken cancellationToken = default)
        => RespRead.Entries(ExecuteAsync(CoverageCommands.Combine("ZINTER", null, keys, weights, aggregate, true).AsList(), cancellationToken), "ZINTER");

    /// <summary>Sends <c>ZDIFF numkeys key... WITHSCORES</c>: members of the first set missing from the others.</summary>
    ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetDifferenceAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
        => RespRead.Entries(ExecuteAsync(CoverageCommands.Combine("ZDIFF", null, keys, null, null, true).AsList(), cancellationToken), "ZDIFF");

    /// <summary>Sends <c>ZUNIONSTORE</c> and returns the destination size.</summary>
    ValueTask<long> StoreSortedSetUnionAsync(string destination, IReadOnlyList<string> keys, IReadOnlyList<double>? weights = null, SortedSetAggregate? aggregate = null, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.Combine("ZUNIONSTORE", destination, keys, weights, aggregate, false).AsList(), cancellationToken), "ZUNIONSTORE");

    /// <summary>Sends <c>ZINTERSTORE</c> and returns the destination size.</summary>
    ValueTask<long> StoreSortedSetIntersectionAsync(string destination, IReadOnlyList<string> keys, IReadOnlyList<double>? weights = null, SortedSetAggregate? aggregate = null, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.Combine("ZINTERSTORE", destination, keys, weights, aggregate, false).AsList(), cancellationToken), "ZINTERSTORE");

    /// <summary>Sends <c>ZDIFFSTORE</c> and returns the destination size.</summary>
    ValueTask<long> StoreSortedSetDifferenceAsync(string destination, IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.Combine("ZDIFFSTORE", destination, keys, null, null, false).AsList(), cancellationToken), "ZDIFFSTORE");

    /// <summary>Sends <c>HRANDFIELD key</c>. Null for a missing key.</summary>
    ValueTask<string?> GetHashRandomFieldAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteAsync(new RespCommand("HRANDFIELD", key).AsList(), cancellationToken));

    /// <summary>Sends <c>HRANDFIELD key count</c>. A negative count may repeat fields.</summary>
    ValueTask<IReadOnlyList<string>> GetHashRandomFieldsAsync(string key, long count, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteAsync(CoverageCommands.RandomMembers("HRANDFIELD", key, count, null).AsList(), cancellationToken), "HRANDFIELD");

    /// <summary>Sends <c>HRANDFIELD key count WITHVALUES</c>.</summary>
    ValueTask<IReadOnlyList<KeyValuePair<string, string>>> GetHashRandomFieldsWithValuesAsync(string key, long count, CancellationToken cancellationToken = default)
        => RespRead.Pairs(ExecuteAsync(CoverageCommands.RandomMembers("HRANDFIELD", key, count, "WITHVALUES").AsList(), cancellationToken), "HRANDFIELD");

    /// <summary>Sends <c>HINCRBYFLOAT</c> and returns the new value.</summary>
    ValueTask<double> IncrementHashFieldByFloatAsync(string key, string field, double delta, CancellationToken cancellationToken = default)
        => RespRead.Double(ExecuteAsync(CoverageCommands.IncrementHashFieldByFloat(key, field, delta).AsList(), cancellationToken), "HINCRBYFLOAT");

    /// <summary>Sends <c>BF.RESERVE key error capacity [EXPANSION n] [NONSCALING]</c>.</summary>
    ValueTask ReserveBloomFilterAsync(string key, double errorRate, long capacity, long? expansion, bool nonScaling = false, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteAsync(CoverageCommands.ReserveBloom(key, errorRate, capacity, expansion, nonScaling).AsList(), cancellationToken));

    /// <summary>Sends <c>BF.MADD</c>: one flag per item, true when it was added (not seen before). A full non-scaling filter throws.</summary>
    ValueTask<IReadOnlyList<bool>> AddBloomFilterManyAsync(string key, params string[] items)
        => RespRead.Flags(ExecuteAsync(CoverageCommands.Members("BF.MADD", key, items).AsList(), CancellationToken.None), "BF.MADD");

    /// <summary>Sends <c>BF.MEXISTS</c>: one flag per item, true when it may have been added.</summary>
    ValueTask<IReadOnlyList<bool>> BloomFilterContainsManyAsync(string key, params string[] items)
        => RespRead.Flags(ExecuteAsync(CoverageCommands.Members("BF.MEXISTS", key, items).AsList(), CancellationToken.None), "BF.MEXISTS");

    /// <summary>Sends <c>BF.INSERT key [CAPACITY] [ERROR] [EXPANSION] [NOCREATE] [NONSCALING] ITEMS item...</c>: one flag per item, true when added.</summary>
    ValueTask<IReadOnlyList<bool>> InsertBloomFilterAsync(string key, IReadOnlyList<string> items, BloomInsertOptions? options = null, CancellationToken cancellationToken = default)
        => RespRead.Flags(ExecuteAsync(CoverageCommands.InsertBloom(key, items, options).AsList(), cancellationToken), "BF.INSERT");

    /// <summary>Sends <c>BF.INFO key</c>.</summary>
    ValueTask<BloomFilterInfo> GetBloomFilterInfoAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.BloomInfo(ExecuteAsync(new RespCommand("BF.INFO", key).AsList(), cancellationToken));

    /// <summary>Sends <c>BF.CARD</c>: items added so far; 0 for a missing key.</summary>
    ValueTask<long> GetBloomFilterCardinalityAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(new RespCommand("BF.CARD", key).AsList(), cancellationToken), "BF.CARD");

    /// <summary>Sends <c>JSON.SET key path json NX|XX</c>. False when the condition or the path skipped the write.</summary>
    ValueTask<bool> JsonSetAsync(string key, string path, string json, SetMode mode, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteAsync(CoverageCommands.JsonSet(key, path, json, mode).AsList(), cancellationToken));

    /// <summary>Sends <c>JSON.OBJLEN</c>. Null when the key or path is absent.</summary>
    ValueTask<long?> JsonObjectLengthAsync(string key, string path = "$", CancellationToken cancellationToken = default)
        => RespRead.OptionalInteger(ExecuteAsync(new RespCommand("JSON.OBJLEN", key, path).AsList(), cancellationToken), "JSON.OBJLEN");

    /// <summary>Sends <c>JSON.STRAPPEND key path json-string</c>; <paramref name="json"/> is a JSON string literal such as <c>"\"abc\""</c>. Returns the new length in bytes.</summary>
    ValueTask<long?> JsonStringAppendAsync(string key, string path, string json, CancellationToken cancellationToken = default)
        => RespRead.OptionalInteger(ExecuteAsync(new RespCommand("JSON.STRAPPEND", key, path, json).AsList(), cancellationToken), "JSON.STRAPPEND");

    /// <summary>Sends <c>JSON.NUMMULTBY</c> and returns the new number as JSON text; null when absent.</summary>
    ValueTask<string?> JsonMultiplyAsync(string key, string path, double by, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteAsync(new RespCommand("JSON.NUMMULTBY", key, path, RespRead.Number(by)).AsList(), cancellationToken));

    /// <summary>Sends <c>JSON.ARRINSERT key path index json...</c> and returns the new length.</summary>
    ValueTask<long?> JsonArrayInsertAsync(string key, string path, long index, params string[] json)
        => RespRead.OptionalInteger(ExecuteAsync(CoverageCommands.JsonArrayInsert(key, path, index, json).AsList(), CancellationToken.None), "JSON.ARRINSERT");

    /// <summary>Sends <c>JSON.ARRPOP key path index</c> and returns the removed element as JSON; null for an empty array.</summary>
    ValueTask<string?> JsonArrayPopAsync(string key, string path = "$", long index = -1, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteAsync(new RespCommand("JSON.ARRPOP", key, path, index).AsList(), cancellationToken));

    /// <summary>Sends <c>JSON.ARRTRIM key path start stop</c> (inclusive) and returns the new length.</summary>
    ValueTask<long?> JsonArrayTrimAsync(string key, string path, long start, long stop, CancellationToken cancellationToken = default)
        => RespRead.OptionalInteger(ExecuteAsync(new RespCommand("JSON.ARRTRIM", key, path, start, stop).AsList(), cancellationToken), "JSON.ARRTRIM");

    /// <summary>Sends <c>JSON.ARRINDEX key path scalar [start stop]</c>; <paramref name="scalar"/> is JSON text. -1 when absent; a stop of 0 means the end.</summary>
    ValueTask<long?> JsonArrayIndexAsync(string key, string path, string scalar, long start = 0, long stop = 0, CancellationToken cancellationToken = default)
        => RespRead.OptionalInteger(ExecuteAsync(CoverageCommands.JsonArrayIndex(key, path, scalar, start, stop).AsList(), cancellationToken), "JSON.ARRINDEX");

    /// <summary>Sends <c>JSON.FORGET</c>, the alias of <c>JSON.DEL</c>.</summary>
    ValueTask<long> JsonForgetAsync(string key, string path = "$", CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(new RespCommand("JSON.FORGET", key, path).AsList(), cancellationToken), "JSON.FORGET");

    /// <summary>Sends <c>JMERGE</c>, the alias of <c>JSON.MERGE</c>. Prefer <c>JsonMergeAsync</c>.</summary>
    ValueTask<bool> JMergeAsync(string key, string path, string patch, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteAsync(new RespCommand("JMERGE", key, path, patch).AsList(), cancellationToken));

    /// <summary>Sends <c>JSON.INDEX CREATE name PREFIX prefix PATH path TYPE type</c> (Ruvio extension; database 0 only). The index builds in the background; <c>JSON.FIND</c> answers <c>ERR index is building</c> until it is ready.</summary>
    ValueTask JsonCreateIndexAsync(string name, string prefix, string path, JsonIndexType type = JsonIndexType.String, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteAsync(CoverageCommands.JsonCreateIndex(name, prefix, path, type).AsList(), cancellationToken));

    /// <summary>Sends <c>JSON.INDEX DROP name</c>. False when the index did not exist.</summary>
    ValueTask<bool> JsonDropIndexAsync(string name, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteAsync(new RespCommand("JSON.INDEX", "DROP", name).AsList(), cancellationToken), "JSON.INDEX");

    /// <summary>Sends <c>JSON.INDEX LIST</c>: one description line per index (name, prefix, path, type, state, keys, memory).</summary>
    ValueTask<IReadOnlyList<string>> JsonListIndexesAsync(CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteAsync(new RespCommand("JSON.INDEX", "LIST").AsList(), cancellationToken), "JSON.INDEX");

    /// <summary>Sends <c>JSON.FIND index VALUE value [COUNT n] [AFTER key]</c> (Ruvio extension): matching keys in ascending order. Page with <paramref name="after"/> set to the last key.</summary>
    ValueTask<IReadOnlyList<string>> JsonFindAsync(string index, string value, int? count = null, string? after = null, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteAsync(CoverageCommands.JsonFind(index, value, count, after).AsList(), cancellationToken), "JSON.FIND");

    /// <summary>Sends <c>RUVIO</c> and returns the server's answer.</summary>
    ValueTask<string> RuvioAsync(CancellationToken cancellationToken = default)
        => RespRead.RequiredText(ExecuteAsync(new RespCommand("RUVIO").AsList(), cancellationToken), "RUVIO");

    /// <summary>Sends <c>RUVIO OWNER</c>: names the shard rule.</summary>
    ValueTask<string> GetRuvioOwnerAsync(CancellationToken cancellationToken = default)
        => RespRead.RequiredText(ExecuteAsync(new RespCommand("RUVIO", "OWNER").AsList(), cancellationToken), "RUVIO");

    /// <summary>Sends <c>COMMAND</c>: the whole command table.</summary>
    ValueTask<RespValue> GetCommandsAsync(CancellationToken cancellationToken = default)
        => ExecuteAsync(new RespCommand("COMMAND").AsList(), cancellationToken);

    /// <summary>Sends <c>COMMAND COUNT</c>.</summary>
    ValueTask<long> GetCommandCountAsync(CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(new RespCommand("COMMAND", "COUNT").AsList(), cancellationToken), "COMMAND");

    /// <summary>Sends <c>COMMAND INFO name...</c> (Redis 7 shape).</summary>
    ValueTask<RespValue> GetCommandInfoAsync(params string[] names)
        => ExecuteAsync(CoverageCommands.Words("COMMAND", "INFO", names).AsList(), CancellationToken.None);

    /// <summary>Sends <c>COMMAND DOCS name...</c>.</summary>
    ValueTask<RespValue> GetCommandDocsAsync(params string[] names)
        => ExecuteAsync(CoverageCommands.Words("COMMAND", "DOCS", names).AsList(), CancellationToken.None);

    /// <summary>Sends <c>LATENCY LATEST</c>. Ruvio records no spike events, so this is empty.</summary>
    ValueTask<IReadOnlyList<RespValue>> GetLatencyLatestAsync(CancellationToken cancellationToken = default)
        => RuvioClient.ItemsOf(ExecuteAsync(new RespCommand("LATENCY", "LATEST").AsList(), cancellationToken));

    /// <summary>Sends <c>WAIT replicas timeout</c> and returns how many replicas reached this shard's last write. Zero timeout waits until shutdown. The shared connection sends nothing else while it waits.</summary>
    ValueTask<long> WaitForReplicasAsync(long replicas, TimeSpan timeout, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.Wait(replicas, timeout).AsList(), cancellationToken), "WAIT");

    /// <summary>Sends <c>AUTH password</c> on the shared connection. Every caller of this client then runs as that user; a reconnect authenticates with <see cref="RuvioClientOptions"/> again.</summary>
    ValueTask AuthenticateAsync(string password, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteAsync(new RespCommand("AUTH", password).AsList(), cancellationToken));

    /// <summary>Sends <c>AUTH username password</c> on the shared connection. Every caller of this client then runs as that user; a reconnect authenticates with <see cref="RuvioClientOptions"/> again.</summary>
    ValueTask AuthenticateAsync(string username, string password, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteAsync(new RespCommand("AUTH", username, password).AsList(), cancellationToken));

    /// <summary>Sends <c>CLIENT KILL ADDR ip:port</c> and returns how many connections were closed.</summary>
    ValueTask<long> KillClientAsync(string address, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(new RespCommand("CLIENT", "KILL", "ADDR", address).AsList(), cancellationToken), "CLIENT");

    /// <summary>Sends <c>IDEM key BEGIN fingerprint owner ttl</c> and reads the decision.</summary>
    ValueTask<IdempotencyBegin> TryBeginIdempotencyAsync(string key, string fingerprint, string owner, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Idempotency(ExecuteAsync(new RespCommand("IDEM", key, "BEGIN", fingerprint, owner, RespRead.Milliseconds(ttl)).AsList(), cancellationToken));
}
