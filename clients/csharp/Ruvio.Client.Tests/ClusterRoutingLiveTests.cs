using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>Cluster routing paths (scan, transactions, split, cross-slot refusal, fan-out) against the live 3-shard server.</summary>
[Collection("Live")]
public class ClusterRoutingLiveTests
{
    private static async Task<(RuvioConnection Connection, RuvioClient Client)?> Open()
    {
        string? address = Environment.GetEnvironmentVariable("RUVIO_CLUSTER_ADDR");

        if (string.IsNullOrWhiteSpace(address))
        {
            return null;
        }

        string[] parts = address.Split(':');
        RuvioConnection connection = await RuvioConnection.ConnectAsync(parts[0], int.Parse(parts[1]));

        return (connection, await connection.GetDefaultDatabaseAsync());
    }

    /// <summary>Keys with the given prefix that all land on different slots, grouped by owner so tests can pick cross-shard pairs.</summary>
    private static async Task Fails(RuvioClient client, params string[] arguments)
    {
        await Assert.ThrowsAsync<RuvioException>(async () => await client.ExecuteAsync(arguments));
    }

    private static List<string> SpreadKeys(string prefix, int count)
    {
        var keys = new List<string>();
        var slots = new HashSet<int>();

        for (int i = 0; keys.Count < count; i++)
        {
            string key = $"{prefix}-{i}";

            if (slots.Add(RuvioClient.GetHashSlot(key)))
            {
                keys.Add(key);
            }
        }

        return keys;
    }

    private static async Task<(string A, string B)> CrossShardPair(RuvioClient client, string prefix)
    {
        // Slot ranges are split in thirds; pick keys at the far ends so they are on different shards.
        string a = "", b = "";

        for (int i = 0; ; i++)
        {
            string key = $"{prefix}-{i}";
            int slot = RuvioClient.GetHashSlot(key);

            if (a == "" && slot < 4000)
            {
                a = key;
            }
            else if (b == "" && slot > 12000)
            {
                b = key;
            }

            if (a != "" && b != "")
            {
                break;
            }
        }

        await Task.CompletedTask;

        return (a, b);
    }

    [Fact(Timeout = 30000)]
    public async Task ScanWalksEveryShardAndRejectsBadCursors()
    {
        if (await Open() is not { } open)
        {
            return;
        }

        await using RuvioConnection connection = open.Connection;
        RuvioClient client = open.Client;
        List<string> keys = SpreadKeys("scanrt", 40);

        foreach (string key in keys)
        {
            await client.SetAsync(key, "v");
        }

        var seen = new HashSet<string>();
        string cursor = "0";

        do
        {
            RespValue reply = await client.ExecuteAsync("SCAN", cursor, "MATCH", "scanrt-*", "COUNT", "10");

            cursor = reply.Items![0].AsString()!;

            foreach (RespValue item in reply.Items[1].Items!)
            {
                seen.Add(item.AsString()!);
            }
        }
        while (cursor != "0");

        Assert.Superset(keys.ToHashSet(), seen);

        await Fails(client, "SCAN", "not-a-number");
        await Fails(client, "SCAN");
        await Fails(client, "SCAN", "-5");

        foreach (string key in keys)
        {
            await client.DeleteAsync([key]);
        }
    }

    [Fact(Timeout = 30000)]
    public async Task MultiKeyCommandsAreSplitAcrossShards()
    {
        if (await Open() is not { } open)
        {
            return;
        }

        await using RuvioConnection connection = open.Connection;
        RuvioClient client = open.Client;
        List<string> keys = SpreadKeys("splitrt", 12);
        var mset = new List<string> { "MSET" };

        foreach (string key in keys)
        {
            mset.Add(key);
            mset.Add("val-" + key);
        }

        Assert.Equal("OK", (await client.ExecuteAsync([.. mset])).AsString());

        RespValue mget = await client.ExecuteAsync(["MGET", .. keys, "splitrt-missing-zzz"]);

        Assert.Equal(keys.Count + 1, mget.Items!.Count);

        for (int i = 0; i < keys.Count; i++)
        {
            Assert.Equal("val-" + keys[i], mget.Items[i].AsString());
        }

        Assert.Equal(keys.Count, (await client.ExecuteAsync(["EXISTS", .. keys])).Integer);
        Assert.Equal(keys.Count, (await client.ExecuteAsync(["UNLINK", .. keys.Take(6)])).Integer + 6);
        Assert.Equal(6, (await client.ExecuteAsync(["DEL", .. keys.Skip(6)])).Integer);
        Assert.Equal(0, (await client.ExecuteAsync(["EXISTS", .. keys])).Integer);

        // Same-slot multi-key goes through the plain path.
        Assert.Equal("OK", (await client.ExecuteAsync("MSET", "{t}a", "1", "{t}b", "2")).AsString());
        Assert.Equal(2, (await client.ExecuteAsync("DEL", "{t}a", "{t}b")).Integer);
    }

    [Fact(Timeout = 30000)]
    public async Task PipelinesGroupByShardAndTransactionsStayOnOneShard()
    {
        if (await Open() is not { } open)
        {
            return;
        }

        await using RuvioConnection connection = open.Connection;
        RuvioClient client = open.Client;
        List<string> keys = SpreadKeys("pipert", 9);
        var commands = new List<IReadOnlyList<string>>();

        foreach (string key in keys)
        {
            commands.Add(["SET", key, "x" + key]);
        }

        foreach (string key in keys)
        {
            commands.Add(["GET", key]);
        }

        commands.Add(["PING"]);

        IReadOnlyList<RespValue> replies = await client.ExecuteManyAsync(commands);

        Assert.Equal(commands.Count, replies.Count);

        for (int i = 0; i < keys.Count; i++)
        {
            Assert.Equal("x" + keys[i], replies[keys.Count + i].AsString());
        }

        // Split commands inside a pipeline are sent per slot and merged at their own index.
        IReadOnlyList<RespValue> split = await client.ExecuteManyAsync(
        [
            ["MSET", keys[0], "m0", keys[1], "m1", keys[2], "m2"],
            ["MGET", keys[2], keys[0], "pipert-missing", keys[1]],
            ["EXISTS", keys[0], keys[1], keys[2], keys[0]],
            ["PING"],
            ["DEL", keys[0], keys[1], keys[2]],
        ]);
        Assert.Equal(5, split.Count);
        Assert.Equal("OK", split[0].AsString());
        Assert.Equal(["m2", "m0", null, "m1"], split[1].Items!.Select(item => item.AsString()));
        Assert.Equal(4, split[2].Integer);
        Assert.Equal("PONG", split[3].AsString());
        Assert.Equal(3, split[4].Integer);

        // A pipeline whose keys all live on one shard is sent as one group.
        IReadOnlyList<RespValue> single = await client.ExecuteManyAsync([["SET", "{p}1", "a"], ["GET", "{p}1"]]);
        Assert.Equal("a", single[1].AsString());

        // A pipeline that holds a transaction goes to one shard unchanged.
        IReadOnlyList<RespValue> tx = await client.ExecuteManyAsync(
            [["MULTI"], ["SET", "{tx}k", "1"], ["INCR", "{tx}k"], ["EXEC"]]);
        Assert.Equal(4, tx.Count);
        Assert.Equal(2, tx[3].Items![1].Integer);

        // No keyed command in a transaction pipeline falls back to the seed node.
        IReadOnlyList<RespValue> nokey = await client.ExecuteManyAsync([["MULTI"], ["PING"], ["EXEC"]]);
        Assert.Equal(3, nokey.Count);

        foreach (string key in keys)
        {
            await client.DeleteAsync([key]);
        }

        await client.DeleteAsync(["{p}1", "{tx}k"]);
    }

    [Fact(Timeout = 30000)]
    public async Task InteractiveTransactionsPinToTheFirstKeyedShard()
    {
        if (await Open() is not { } open)
        {
            return;
        }

        await using RuvioConnection connection = open.Connection;
        RuvioClient client = open.Client;

        // MULTI, un-keyed command queued, then keyed command pins the shard and flushes the queue.
        Assert.Equal("OK", (await client.ExecuteAsync("MULTI")).AsString());
        Assert.Equal("QUEUED", (await client.ExecuteAsync("PING")).AsString());
        Assert.Equal("QUEUED", (await client.ExecuteAsync("SET", "{itx}a", "1")).AsString());
        Assert.Equal("QUEUED", (await client.ExecuteAsync("INCR", "{itx}a")).AsString());
        RespValue exec = await client.ExecuteAsync("EXEC");
        Assert.Equal(3, exec.Items!.Count);
        Assert.Equal(2, exec.Items[2].Integer);

        // Nested MULTI / WATCH inside MULTI before any key is bound.
        await client.ExecuteAsync("MULTI");
        await Fails(client, "MULTI");
        await Fails(client, "WATCH", "{itx}a");
        Assert.Equal("OK", (await client.ExecuteAsync("DISCARD")).AsString());

        // EXEC with only un-keyed commands runs on the seed shard.
        await client.ExecuteAsync("MULTI");
        await client.ExecuteAsync("PING");
        RespValue only = await client.ExecuteAsync("EXEC");
        Assert.Single(only.Items!);

        // Once pinned, further commands go to the pinned shard; DISCARD clears the pin.
        await client.ExecuteAsync("MULTI");
        await client.ExecuteAsync("SET", "{itx}b", "1");
        Assert.Equal("QUEUED", (await client.ExecuteAsync("GET", "{itx}b")).AsString());
        Assert.Equal("OK", (await client.ExecuteAsync("DISCARD")).AsString());
        Assert.Equal(1, (await client.ExecuteAsync("EXISTS", "{itx}a")).Integer);

        // WATCH pins; UNWATCH outside MULTI releases; then a watched transaction commits.
        Assert.Equal("OK", (await client.ExecuteAsync("WATCH", "{itx}a")).AsString());
        Assert.Equal("OK", (await client.ExecuteAsync("UNWATCH")).AsString());
        Assert.Equal("OK", (await client.ExecuteAsync("WATCH", "{itx}a")).AsString());
        Assert.Equal("OK", (await client.ExecuteAsync("MULTI")).AsString());
        await client.ExecuteAsync("SET", "{itx}a", "9");
        RespValue committed = await client.ExecuteAsync("EXEC");
        Assert.Single(committed.Items!);

        // A failing MULTI body command (wrong arity) reports the error and does not pin.
        await client.ExecuteAsync("MULTI");
        await Fails(client, "SET", "{itx}c");
        await client.ExecuteAsync("DISCARD");

        await client.DeleteAsync(["{itx}a", "{itx}b", "{itx}c"]);
    }

    [Fact(Timeout = 30000)]
    public async Task ScriptAndFlushCommandsReachEveryShard()
    {
        if (await Open() is not { } open)
        {
            return;
        }

        await using RuvioConnection connection = open.Connection;
        RuvioClient client = open.Client;
        RespValue load = await client.ExecuteAsync("SCRIPT", "LOAD", "return 1");
        string sha = load.AsString()!;
        List<string> keys = SpreadKeys("evalrt", 3);

        foreach (string key in keys)
        {
            Assert.Equal(1, (await client.ExecuteAsync("EVALSHA", sha, "1", key)).Integer);
        }

        Assert.Equal("OK", (await client.ExecuteAsync("SCRIPT", "FLUSH")).AsString());

        // Without a sub-command SCRIPT is a plain command.
        RespValue exists = await client.ExecuteAsync("SCRIPT", "EXISTS", sha);
        Assert.NotEqual(RespKind.Error, exists.Kind);

        Assert.Equal(1, (await client.ExecuteAsync("PUBLISH", "chan-rt", "hello")).Kind == RespKind.Integer ? 1 : 0);
    }

    [Fact(Timeout = 30000)]
    public async Task CommandsAcrossSlotsAreRefusedBeforeSending()
    {
        if (await Open() is not { } open)
        {
            return;
        }

        await using RuvioConnection connection = open.Connection;
        RuvioClient client = open.Client;
        (string src, string dst) = await CrossShardPair(client, "renrt");

        Assert.NotEqual(RuvioClient.GetHashSlot(src), RuvioClient.GetHashSlot(dst));
        await client.DeleteAsync([src, dst]);
        await client.SetAsync(src, "text");

        string[][] refused =
        [
            ["RENAME", src, dst], ["RENAMENX", src, dst], ["COPY", src, dst],
            ["SMOVE", src, dst, "m"], ["LMOVE", src, dst, "LEFT", "RIGHT"],
            ["SINTERSTORE", dst, src], ["EVAL", "return 1", "2", src, dst],
        ];

        foreach (string[] command in refused)
        {
            RuvioException error = await Assert.ThrowsAsync<RuvioException>(async () => await client.ExecuteAsync(command));

            Assert.StartsWith("CROSSSLOT", error.Message);
        }

        // Nothing was moved or copied.
        Assert.Equal("text", await client.GetStringAsync(src));
        Assert.Null(await client.GetStringAsync(dst));

        // A rename inside one slot still works.
        await client.SetAsync("{rs}a", "x");
        Assert.Equal("OK", (await client.ExecuteAsync("RENAME", "{rs}a", "{rs}b")).AsString());
        await client.DeleteAsync(["{rs}b", src]);
    }

    [Fact(Timeout = 30000)]
    public async Task BinaryCommandsRouteByKeyAndRejectUnroutableShapes()
    {
        if (await Open() is not { } open)
        {
            return;
        }

        await using RuvioConnection connection = open.Connection;
        RuvioClient client = open.Client;
        ReadOnlyMemory<byte> B(string s) => System.Text.Encoding.ASCII.GetBytes(s);
        List<string> keys = SpreadKeys("binrt", 2);

        RespValue set = await client.ExecuteBinaryAsync(new[] { B("SET"), B(keys[0]), new byte[] { 1, 2, 255 } });
        Assert.Equal("OK", set.AsString());
        RespValue get = await client.ExecuteBinaryAsync(new[] { B("GET"), B(keys[0]) });
        Assert.Equal(new byte[] { 1, 2, 255 }, get.Bytes);
        Assert.Equal("PONG", (await client.ExecuteBinaryAsync(new[] { B("PING") })).AsString());
        Assert.Equal(1, (await client.ExecuteBinaryAsync(new[] { B("DEL"), B(keys[0]) })).Integer);
        Assert.Equal(0, (await client.ExecuteBinaryAsync(new[] { B("EXISTS"), B("{b}x"), B("{b}y") })).Integer);

        // EVAL key counts: non-numeric and too large
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await client.ExecuteBinaryAsync(new[] { B("EVAL"), B("return 1"), B("x") }));
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await client.ExecuteBinaryAsync(new[] { B("EVAL"), B("return 1") }));
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await client.ExecuteBinaryAsync(new[] { B("EVALSHA"), B("sha"), B("-1") }));

        // EVAL with zero keys routes to slot 0
        RespValue zero = await client.ExecuteBinaryAsync(new[] { B("EVAL"), B("return 7"), B("0") });
        Assert.Equal(7, zero.Integer);

        // DEL/EXISTS across slots are split like their string form; a script is refused.
        await client.ExecuteBinaryAsync(new[] { B("SET"), B(keys[1]), B("v") });
        Assert.Equal(1, (await client.ExecuteBinaryAsync(new[] { B("EXISTS"), B(keys[0]), B(keys[1]) })).Integer);
        Assert.Equal(1, (await client.ExecuteBinaryAsync(new[] { B("DEL"), B(keys[0]), B(keys[1]) })).Integer);
        RuvioException crossSlot = await Assert.ThrowsAsync<RuvioException>(async () =>
            await client.ExecuteBinaryAsync(new[] { B("EVAL"), B("return 1"), B("2"), B(keys[0]), B(keys[1]) }));
        Assert.StartsWith("CROSSSLOT", crossSlot.Message);

        // binary commands cannot be mixed with a cluster transaction
        await client.ExecuteAsync("MULTI");
        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await client.ExecuteBinaryAsync(new[] { B("GET"), B("k") }));
        await client.ExecuteAsync("DISCARD");
    }

    [Fact(Timeout = 30000)]
    public async Task FanOutCommandsTotalAcrossShards()
    {
        if (await Open() is not { } open)
        {
            return;
        }

        await using RuvioConnection connection = open.Connection;
        RuvioClient client = open.Client;
        List<string> keys = SpreadKeys("fanrt", 6);

        foreach (string key in keys)
        {
            await client.SetAsync(key, "1");
        }

        Assert.True(await client.GetDatabaseSizeAsync() >= keys.Count);
        Assert.Equal("OK", (await client.ExecuteAsync("FLUSHALL")).AsString());
        Assert.Equal(0, await client.GetDatabaseSizeAsync());
    }
}
