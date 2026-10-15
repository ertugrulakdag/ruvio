using System.Reflection;
using System.Text.Json;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>
/// Reads docs/compatibility-matrix.json and fails when a command has no typed method. The table maps each
/// matrix command to the method that sends it; the live tests in LiveCommandTests exercise them on a server.
/// </summary>
public class MatrixCoverageTests
{
    internal static readonly Dictionary<string, string> Methods = new(StringComparer.Ordinal)
    {
        ["RUVIO"] = "RuvioAsync",
        ["PING"] = "PingAsync",
        ["HELLO"] = "HelloAsync",
        ["AUTH"] = "AuthenticateAsync",
        ["ACL WHOAMI"] = "GetAclUserNameAsync",
        ["ACL USERS"] = "GetAclUsersAsync",
        ["ACL LIST"] = "GetAclListAsync",
        ["ACL GETUSER"] = "GetAclUserAsync",
        ["ACL CAT"] = "GetAclCategoriesAsync",
        ["ACL SETUSER"] = "SetAclUserAsync",
        ["ACL LOAD"] = "LoadAclAsync",
        ["ACL SAVE"] = "SaveAclAsync",
        ["GET"] = "GetStringAsync",
        ["SETV"] = "SetVersionAsync",
        ["TAKE"] = "TakeAsync",
        ["DELEX"] = "DeleteIfEqualAsync",
        ["GETEX"] = "GetExpireAsync",
        ["LIMIT"] = "LimitAsync",
        ["IDEM"] = "BeginIdempotencyAsync",
        ["ONCE"] = "OnceAsync",
        ["LEASE"] = "LeaseAsync",
        ["SEMAPHORE"] = "AcquireSemaphoreAsync",
        ["RELEASE"] = "ReleaseAsync",
        ["CHANGES"] = "StartChangesAsync",
        ["SET"] = "SetAsync",
        ["MGET"] = "GetManyAsync",
        ["MSET"] = "SetManyAsync",
        ["GETSET"] = "GetSetAsync",
        ["APPEND"] = "AppendAsync",
        ["STRLEN"] = "GetStringLengthAsync",
        ["INCR"] = "IncrementAsync",
        ["DECR"] = "DecrementAsync",
        ["INCRBY"] = "IncrementWithLimitAsync",
        ["DECRBY"] = "DecrementByAsync",
        ["DEL"] = "DeleteAsync",
        ["UNLINK"] = "UnlinkAsync",
        ["EXISTS"] = "ExistsAsync",
        ["TYPE"] = "GetKeyTypeAsync",
        ["RENAME"] = "RenameAsync",
        ["SCAN"] = "ScanAsync",
        ["DBSIZE"] = "GetDatabaseSizeAsync",
        ["SELECT"] = "RuvioConnection.GetDatabaseAsync",
        ["FLUSHDB"] = "FlushDatabaseAsync",
        ["FLUSHALL"] = "FlushAllDatabasesAsync",
        ["SWAPDB"] = "SwapDatabasesAsync",
        ["MOVE"] = "MoveKeyAsync",
        ["EXPIRE"] = "ExpireAsync",
        ["PEXPIRE"] = "ExpireAsync",
        ["EXPIREAT"] = "ExpireAtAsync",
        ["PEXPIREAT"] = "ExpireAtAsync",
        ["TTL"] = "GetTimeToLiveAsync",
        ["PTTL"] = "GetTimeToLiveMillisecondsAsync",
        ["PERSIST"] = "PersistAsync",
        ["LPUSH"] = "PushListLeftAsync",
        ["LPUSHCAP"] = "PushListLeftCappedAsync",
        ["RPUSHCAP"] = "PushListRightCappedAsync",
        ["RPUSH"] = "PushListRightAsync",
        ["LPOP"] = "PopListLeftAsync",
        ["RPOP"] = "PopListRightAsync",
        ["LLEN"] = "GetListLengthAsync",
        ["LINDEX"] = "GetListItemAsync",
        ["LRANGE"] = "GetListRangeAsync",
        ["LTRIM"] = "TrimListAsync",
        ["BLPOP"] = "PopListLeftBlockingAsync",
        ["BRPOP"] = "PopListRightBlockingAsync",
        ["SADD"] = "AddSetMembersAsync",
        ["SREM"] = "RemoveSetMembersAsync",
        ["SISMEMBER"] = "SetContainsAsync",
        ["SCARD"] = "GetSetCountAsync",
        ["SMEMBERS"] = "GetSetMembersAsync",
        ["SINTER"] = "GetSetIntersectionAsync",
        ["SUNION"] = "GetSetUnionAsync",
        ["SDIFF"] = "GetSetDifferenceAsync",
        ["SINTERSTORE"] = "StoreSetIntersectionAsync",
        ["SUNIONSTORE"] = "StoreSetUnionAsync",
        ["SDIFFSTORE"] = "StoreSetDifferenceAsync",
        ["SMOVE"] = "MoveSetMemberAsync",
        ["SPOP"] = "PopSetMemberAsync",
        ["SRANDMEMBER"] = "GetSetRandomMemberAsync",
        ["HSET"] = "SetHashFieldsAsync",
        ["HGET"] = "GetHashFieldAsync",
        ["HDEL"] = "DeleteHashFieldsAsync",
        ["HLEN"] = "GetHashLengthAsync",
        ["HGETALL"] = "GetHashAllAsync",
        ["HMGET"] = "GetHashFieldsAsync",
        ["HEXISTS"] = "HashFieldExistsAsync",
        ["HKEYS"] = "GetHashFieldNamesAsync",
        ["HVALS"] = "GetHashFieldValuesAsync",
        ["HINCRBY"] = "IncrementHashFieldAsync",
        ["HSETNX"] = "SetHashFieldIfAbsentAsync",
        ["HSTRLEN"] = "GetHashFieldLengthAsync",
        ["HSCAN"] = "ScanHashAsync",
        ["HEXPIRE"] = "ExpireHashFieldsAsync",
        ["HPEXPIRE"] = "ExpireHashFieldsAsync",
        ["HEXPIREAT"] = "ExpireHashFieldsAtAsync",
        ["HPEXPIREAT"] = "ExpireHashFieldsAtAsync",
        ["HTTL"] = "GetHashFieldTimeToLiveAsync",
        ["HPTTL"] = "GetHashFieldTimeToLiveMillisecondsAsync",
        ["HEXPIRETIME"] = "GetHashFieldExpireTimeAsync",
        ["HPEXPIRETIME"] = "GetHashFieldExpireTimeMillisecondsAsync",
        ["HPERSIST"] = "PersistHashFieldsAsync",
        ["ZADD"] = "AddSortedSetAsync",
        ["ZRANGE"] = "GetSortedSetRangeByRankAsync",
        ["ZREVRANGE"] = "GetSortedSetRangeByRankAsync",
        ["ZRANGEBYSCORE"] = "GetSortedSetRangeByScoreWithScoresAsync",
        ["ZRANGEBYLEX"] = "GetSortedSetRangeByLexAsync",
        ["ZREVRANGEBYLEX"] = "GetSortedSetRangeByLexAsync",
        ["ZREM"] = "RemoveSortedSetMembersAsync",
        ["ZINCRBY"] = "IncrementSortedSetAsync",
        ["ZCARD"] = "GetSortedSetCountAsync",
        ["ZSCORE"] = "GetSortedSetScoreAsync",
        ["ZRANK"] = "GetSortedSetRankAsync",
        ["ZREVRANK"] = "GetSortedSetRankAsync",
        ["ZPOPMIN"] = "PopSortedSetMinAsync",
        ["ZPOPMAX"] = "PopSortedSetMaxAsync",
        ["BZPOPMIN"] = "PopSortedSetMinBlockingAsync",
        ["BZPOPMAX"] = "PopSortedSetMaxBlockingAsync",
        ["ZREMRANGEBYRANK"] = "RemoveSortedSetRangeByRankAsync",
        ["ZREMRANGEBYSCORE"] = "RemoveSortedSetRangeByScoreAsync",
        ["ZREMRANGEBYLEX"] = "RemoveSortedSetRangeByLexAsync",
        ["GEOADD"] = "AddGeoAsync",
        ["GEODIST"] = "GeoDistanceAsync",
        ["GEOHASH"] = "GeoHashAsync",
        ["GEOPOS"] = "GeoPositionAsync",
        ["GEOSEARCH"] = "GeoSearchAsync",
        ["GEOSEARCHSTORE"] = "GeoSearchStoreAsync",
        ["GEORADIUS"] = "GeoRadiusAsync",
        ["GEORADIUSBYMEMBER"] = "GeoRadiusByMemberAsync",
        ["BF.RESERVE"] = "ReserveBloomFilterAsync",
        ["BF.ADD"] = "AddBloomFilterAsync",
        ["BF.MADD"] = "AddBloomFilterManyAsync",
        ["BF.INSERT"] = "InsertBloomFilterAsync",
        ["BF.INFO"] = "GetBloomFilterInfoAsync",
        ["BF.CARD"] = "GetBloomFilterCardinalityAsync",
        ["BF.EXISTS"] = "BloomFilterContainsAsync",
        ["BF.MEXISTS"] = "BloomFilterContainsManyAsync",
        ["JSON.GET"] = "JsonGetAsync",
        ["JSON.INDEX"] = "JsonCreateIndexAsync",
        ["JSON.FIND"] = "JsonFindAsync",
        ["JSON.MGET"] = "JsonGetManyAsync",
        ["JSON.SET"] = "JsonSetAsync",
        ["JSON.DEL"] = "JsonDeleteAsync",
        ["XADD"] = "AddStreamEntryAsync",
        ["XLEN"] = "GetStreamLengthAsync",
        ["XRANGE"] = "GetStreamRangeAsync",
        ["XREVRANGE"] = "GetStreamRangeAsync",
        ["XREAD"] = "GetStreamUpdatesAsync",
        ["XDEL"] = "DeleteStreamEntriesAsync",
        ["XSETID"] = "SetStreamIdAsync",
        ["XTRIM"] = "TrimStreamAsync",
        ["XGROUP CREATE"] = "CreateStreamGroupAsync",
        ["XGROUP CREATECONSUMER"] = "CreateStreamConsumerAsync",
        ["XREADGROUP"] = "GetStreamGroupUpdatesAsync",
        ["XGROUP DESTROY"] = "DestroyStreamGroupAsync",
        ["XGROUP SETID"] = "SetStreamGroupIdAsync",
        ["XGROUP DELCONSUMER"] = "DeleteStreamGroupConsumerAsync",
        ["XACK"] = "AcknowledgeStreamAsync",
        ["XPENDING"] = "GetStreamPendingAsync",
        ["XAUTOCLAIM"] = "AutoClaimStreamAsync",
        ["XCLAIM"] = "ClaimStreamEntriesAsync",
        ["XINFO STREAM"] = "GetStreamInfoAsync",
        ["XINFO GROUPS"] = "GetStreamGroupsAsync",
        ["XINFO CONSUMERS"] = "GetStreamConsumersAsync",
        ["INFO"] = "GetInfoAsync",
        ["WAIT"] = "WaitForReplicasAsync",
        ["CONFIG GET"] = "GetConfigAsync",
        ["SAVE"] = "SaveAsync",
        ["BGSAVE"] = "SaveInBackgroundAsync",
        ["MULTI"] = "BeginTransactionAsync",
        ["EXEC"] = "ExecuteTransactionAsync",
        ["DISCARD"] = "DiscardTransactionAsync",
        ["WATCH"] = "WatchAsync",
        ["UNWATCH"] = "UnwatchAsync",
        ["EVAL"] = "EvaluateAsync",
        ["EVALSHA"] = "EvaluateShaAsync",
        ["SCRIPT LOAD"] = "LoadScriptAsync",
        ["SCRIPT EXISTS"] = "ScriptExistsAsync",
        ["SCRIPT FLUSH"] = "FlushScriptsAsync",
        ["SCRIPT KILL"] = "KillScriptAsync",
        ["FCALL"] = "CallFunctionAsync",
        ["FUNCTION LOAD"] = "LoadFunctionAsync",
        ["FUNCTION LIST"] = "GetFunctionListAsync",
        ["FUNCTION DELETE"] = "DeleteFunctionAsync",
        ["CLUSTER SLOTS"] = "GetClusterSlotsAsync",
        ["CLUSTER NODES"] = "GetClusterNodesAsync",
        ["CLUSTER SHARDS"] = "GetClusterShardsAsync",
        ["CLUSTER INFO"] = "GetClusterInfoAsync",
        ["CLUSTER MYID"] = "GetClusterMyIdAsync",
        ["CLUSTER KEYSLOT"] = "GetClusterKeySlotAsync",
        ["READONLY"] = "ReadOnlyAsync",
        ["READWRITE"] = "ReadWriteAsync",
        ["SPUBLISH"] = "PublishShardedAsync",
        ["SSUBSCRIBE"] = "SubscribeShardedAsync",
        ["SUNSUBSCRIBE"] = "UnsubscribeShardedAsync",
        ["PUBLISH"] = "PublishAsync",
        ["SUBSCRIBE"] = "SubscribeAsync",
        ["UNSUBSCRIBE"] = "UnsubscribeAsync",
        ["PSUBSCRIBE"] = "SubscribePatternsAsync",
        ["PUNSUBSCRIBE"] = "UnsubscribePatternsAsync",
        ["CLIENT GETNAME"] = "GetConnectionNameAsync",
        ["CLIENT SETNAME"] = "SetConnectionNameAsync",
        ["CLIENT TRACKING"] = "SetTrackingAsync",
        ["SSCAN"] = "ScanSetAsync",
        ["ZSCAN"] = "ScanSortedSetAsync",
        ["JSON.MERGE"] = "JsonMergeAsync",
        ["JSON.TYPE"] = "JsonTypeAsync",
        ["JSON.STRLEN"] = "JsonStringLengthAsync",
        ["JSON.ARRLEN"] = "JsonArrayLengthAsync",
        ["JSON.OBJKEYS"] = "JsonObjectKeysAsync",
        ["JSON.NUMINCRBY"] = "JsonIncrementAsync",
        ["JSON.TOGGLE"] = "JsonToggleAsync",
        ["JSON.ARRAPPEND"] = "JsonArrayAppendAsync",
        ["JSON.CLEAR"] = "JsonClearAsync",
        ["JSON.OBJLEN"] = "JsonObjectLengthAsync",
        ["JSON.STRAPPEND"] = "JsonStringAppendAsync",
        ["JSON.NUMMULTBY"] = "JsonMultiplyAsync",
        ["JSON.ARRINSERT"] = "JsonArrayInsertAsync",
        ["JSON.ARRPOP"] = "JsonArrayPopAsync",
        ["JSON.ARRTRIM"] = "JsonArrayTrimAsync",
        ["JSON.ARRINDEX"] = "JsonArrayIndexAsync",
        ["JSON.FORGET"] = "JsonForgetAsync",
        ["JMERGE"] = "JMergeAsync",
        ["COMMAND"] = "GetCommandsAsync",
        ["SETBIT"] = "SetBitAsync",
        ["BITFIELD"] = "BitFieldAsync",
        ["BITFIELD_RO"] = "BitFieldReadOnlyAsync",
        ["GETBIT"] = "GetBitAsync",
        ["BITCOUNT"] = "BitCountAsync",
        ["BITPOS"] = "BitPositionAsync",
        ["BITOP"] = "BitOperationAsync",
        ["PFADD"] = "HyperLogLogAddAsync",
        ["PFCOUNT"] = "HyperLogLogCountAsync",
        ["PFMERGE"] = "HyperLogLogMergeAsync",
        ["FILLGET"] = "FillGetAsync",
        ["FILLSET"] = "FillSetAsync",
        ["SETEX"] = "SetExpireAsync",
        ["PSETEX"] = "SetExpireMillisecondsAsync",
        ["SETNX"] = "SetIfNotExistsAsync",
        ["GETDEL"] = "GetDeleteAsync",
        ["MSETNX"] = "SetManyIfNotExistsAsync",
        ["INCRBYFLOAT"] = "IncrementByFloatAsync",
        ["GETRANGE"] = "GetRangeAsync",
        ["SUBSTR"] = "GetSubstringAsync",
        ["SETRANGE"] = "SetRangeAsync",
        ["SMISMEMBER"] = "SetContainsManyAsync",
        ["SINTERCARD"] = "GetSetIntersectionCountAsync",
        ["ZCOUNT"] = "CountSortedSetByScoreAsync",
        ["ZLEXCOUNT"] = "CountSortedSetByLexAsync",
        ["ZMSCORE"] = "GetSortedSetScoresAsync",
        ["ZUNIONSTORE"] = "StoreSortedSetUnionAsync",
        ["ZINTERSTORE"] = "StoreSortedSetIntersectionAsync",
        ["ZDIFFSTORE"] = "StoreSortedSetDifferenceAsync",
        ["ZUNION"] = "GetSortedSetUnionAsync",
        ["ZINTER"] = "GetSortedSetIntersectionAsync",
        ["ZDIFF"] = "GetSortedSetDifferenceAsync",
        ["ZRANDMEMBER"] = "GetSortedSetRandomMemberAsync",
        ["HRANDFIELD"] = "GetHashRandomFieldAsync",
        ["HINCRBYFLOAT"] = "IncrementHashFieldByFloatAsync",
        ["LINSERT"] = "InsertListAsync",
        ["LSET"] = "SetListItemAsync",
        ["LREM"] = "RemoveListItemsAsync",
        ["LPOS"] = "GetListPositionAsync",
        ["LMOVE"] = "MoveListItemAsync",
        ["RPOPLPUSH"] = "PopListRightPushLeftAsync",
        ["BLMOVE"] = "MoveListItemBlockingAsync",
        ["LPUSHX"] = "PushListLeftIfExistsAsync",
        ["RPUSHX"] = "PushListRightIfExistsAsync",
        ["LMPOP"] = "PopListManyAsync",
        ["BLMPOP"] = "PopListManyBlockingAsync",
        ["KEYS"] = "GetKeysAsync",
        ["RANDOMKEY"] = "GetRandomKeyAsync",
        ["TOUCH"] = "TouchAsync",
        ["RENAMENX"] = "RenameIfNotExistsAsync",
        ["EXPIRETIME"] = "GetExpireTimeAsync",
        ["PEXPIRETIME"] = "GetExpireTimeMillisecondsAsync",
        ["COPY"] = "CopyAsync",
        ["ECHO"] = "EchoAsync",
        ["OBJECT"] = "GetObjectEncodingAsync",
        ["MEMORY"] = "GetMemoryUsageAsync",
        ["CLIENT ID"] = "GetClientIdAsync",
        ["CLIENT LIST"] = "GetClientListAsync",
        ["CLIENT INFO"] = "GetClientInfoAsync",
        ["CLIENT KILL"] = "KillClientAsync",
        ["SLOWLOG"] = "GetSlowLogLengthAsync",
        ["LATENCY"] = "GetLatencyLatestAsync",
        ["DUMP"] = "DumpAsync",
        ["RESTORE"] = "RestoreAsync",
    };

    [Fact]
    public void EveryMatrixCommandMapsToATypedMethod()
    {
        string matrix = FindMatrix();
        using JsonDocument document = JsonDocument.Parse(File.ReadAllText(matrix));
        List<string> commands = document.RootElement.GetProperty("commands").EnumerateArray()
            .Select(command => command.GetProperty("name").GetString()!)
            .ToList();
        List<string> missing = [];

        foreach (string command in commands)
        {
            if (!Methods.TryGetValue(command, out string? method))
            {
                missing.Add(command + " (not in the table)");
                continue;
            }

            if (!Exists(method))
            {
                missing.Add(command + " -> " + method + " (no such public method)");
            }
        }

        List<string> stale = Methods.Keys.Except(commands, StringComparer.Ordinal).ToList();

        Assert.True(missing.Count == 0, "Matrix commands without a typed method: " + string.Join(", ", missing));
        Assert.True(stale.Count == 0, "Table entries that are not in the matrix: " + string.Join(", ", stale));
        Assert.Equal(commands.Count, commands.Count(command => Methods.ContainsKey(command)));
    }

    [Fact]
    public void EveryMappedMethodIsOnIRuvioClientOrTheConnection()
    {
        foreach (string method in Methods.Values.Distinct())
        {
            if (method.StartsWith("RuvioConnection.", StringComparison.Ordinal))
            {
                Assert.NotNull(typeof(IRuvioConnection).GetMethod(method["RuvioConnection.".Length..]));
                continue;
            }

            Assert.Contains(typeof(IRuvioClient).GetMethods(), info => info.Name == method);
        }
    }

    private static bool Exists(string method)
    {
        const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance;

        return method.StartsWith("RuvioConnection.", StringComparison.Ordinal)
            ? typeof(RuvioConnection).GetMethods(flags).Any(info => info.Name == method["RuvioConnection.".Length..])
            : typeof(RuvioClient).GetMethods(flags).Any(info => info.Name == method);
    }

    internal static string FindMatrix()
    {
        string? directory = AppContext.BaseDirectory;

        for (int depth = 0; depth < 10 && directory is not null; depth++)
        {
            string candidate = Path.Combine(directory, "docs", "compatibility-matrix.json");

            if (File.Exists(candidate))
            {
                return candidate;
            }

            directory = Directory.GetParent(directory)?.FullName;
        }

        throw new FileNotFoundException("docs/compatibility-matrix.json was not found above the test output.");
    }
}
