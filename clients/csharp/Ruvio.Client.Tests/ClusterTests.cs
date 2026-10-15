using System.Net;
using System.Net.Sockets;
using System.Text;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

public class ClusterTests
{
    [Fact]
    public void HashSlotMatchesRedisAndHonorsAHashTag()
    {
        Assert.Equal(12739, RuvioClient.GetHashSlot("123456789"));
        Assert.Equal(11058, RuvioClient.GetHashSlot("somekey"));
        Assert.Equal(3443, RuvioClient.GetHashSlot("{user1000}.following"));
        Assert.Equal(RuvioClient.GetHashSlot("user:{42}:name"), RuvioClient.GetHashSlot("cart:{42}"));
        Assert.Equal(RuvioClient.GetHashSlot("plain"), ClusterKeys.Slot("plain"));
    }

    [Fact]
    public void CommandKeysFollowTheRedisPositions()
    {
        Assert.Equal(["a", "b"], ClusterKeys.Of("MGET", ["MGET", "a", "b"]));
        Assert.Equal(["a", "c"], ClusterKeys.Of("MSET", ["MSET", "a", "1", "c", "2"]));
        Assert.Equal(["a", "b"], ClusterKeys.Of("DEL", ["DEL", "a", "b"]));
        Assert.Equal(["k1", "k2"], ClusterKeys.Of("EVAL", ["EVAL", "return 1", "2", "k1", "k2", "arg"]));
        Assert.Equal(["s1", "s2"], ClusterKeys.Of("XREAD", ["XREAD", "STREAMS", "s1", "s2", "0-0", "0-0"]));
        Assert.Empty(ClusterKeys.Of("PING", ["PING"]));
        Assert.Empty(ClusterKeys.Of("CHANGES", ["CHANGES", "START"]));
        Assert.Equal(["lock"], ClusterKeys.Of("LEASE", ["LEASE", "lock", "5000"]));
        Assert.Equal(["login"], ClusterKeys.Of("LIMIT", ["LIMIT", "login", "5", "60000"]));
        Assert.Equal(["pay"], ClusterKeys.Of("ONCE", ["ONCE", "pay", "60000"]));
        Assert.Equal(["s"], ClusterKeys.Of("XADD", ["XADD", "s", "DELAY", "5", "*", "f", "v"]));
        Assert.Empty(ClusterKeys.Of("PUBLISH", ["PUBLISH", "chan", "hi"]));
        Assert.Equal(["chan"], ClusterKeys.Of("SPUBLISH", ["SPUBLISH", "chan", "hi"]));
    }

    [Fact]
    public void FirstKeyMatchesTheFullKeyListWithoutBuildingIt()
    {
        string[][] commands =
        [
            ["GET", "a"], ["MGET", "a", "b"], ["MSET", "a", "1", "c", "2"], ["RENAME", "a", "b"],
            ["EVAL", "return 1", "2", "k1", "k2"], ["EVAL", "return 1", "0"], ["XREAD", "STREAMS", "s1", "0-0"],
            ["BLPOP", "l1", "l2", "0"], ["OBJECT", "ENCODING", "k"], ["ZUNIONSTORE", "d", "2", "a", "b"],
            ["PING"], ["PUBLISH", "chan", "hi"], ["LIMIT", "login", "5", "60000"],
            ["LMPOP", "2", "a", "b", "LEFT"], ["BLMPOP", "0", "1", "a", "LEFT"],
        ];

        foreach (string[] command in commands)
        {
            IReadOnlyList<string> keys = ClusterKeys.Of(command[0], command);
            Assert.Equal(keys.Count > 0 ? keys[0] : null, ClusterKeys.First(command[0], command));
        }

        Assert.Equal(["a", "b"], ClusterKeys.Of("LMPOP", ["LMPOP", "2", "a", "b", "LEFT"]));
        Assert.Equal(["a"], ClusterKeys.Of("BLMPOP", ["BLMPOP", "0", "1", "a", "LEFT"]));
        Assert.True(ClusterKeys.IsSingleKey("GET"));
        Assert.True(ClusterKeys.IsSingleKey("IDEM"));
        Assert.False(ClusterKeys.IsSingleKey("MGET"));
        Assert.False(ClusterKeys.IsSingleKey("EVAL"));
        Assert.False(ClusterKeys.IsSingleKey("OBJECT"));
        Assert.False(ClusterKeys.IsSingleKey("PING"));

        // Coverage-pass commands whose first argument is not the key.
        Assert.Equal(["dest", "a", "b"], ClusterKeys.Of("BITOP", ["BITOP", "AND", "dest", "a", "b"]));
        Assert.Equal(["d", "s"], ClusterKeys.Of("BITOP", ["BITOP", "NOT", "d", "s"]));
        Assert.Equal(["x", "y"], ClusterKeys.Of("SINTERCARD", ["SINTERCARD", "2", "x", "y", "LIMIT", "5"]));
        Assert.Equal(["d1", "d2"], ClusterKeys.Of("JSON.MGET", ["JSON.MGET", "d1", "d2", "$.a"]));
        Assert.Empty(ClusterKeys.Of("JSON.FIND", ["JSON.FIND", "idx", "VALUE", "v"]));
        Assert.Empty(ClusterKeys.Of("JSON.INDEX", ["JSON.INDEX", "LIST"]));
        Assert.Empty(ClusterKeys.Of("RUVIO", ["RUVIO", "OWNER"]));
        Assert.Equal(["k"], ClusterKeys.Of("DELEX", ["DELEX", "k", "IFEQ", "t"]));
        Assert.Equal(["l"], ClusterKeys.Of("LPUSHCAP", ["LPUSHCAP", "l", "3", "v"]));
        Assert.Equal(["bf"], ClusterKeys.Of("BF.MADD", ["BF.MADD", "bf", "a"]));
        Assert.False(ClusterKeys.IsSingleKey("BITOP"));
        Assert.False(ClusterKeys.IsSingleKey("SINTERCARD"));
        Assert.False(ClusterKeys.IsSingleKey("JSON.FIND"));
        Assert.True(ClusterKeys.IsSingleKey("DELEX"));
        Assert.True(ClusterKeys.IsSingleKey("SSCAN"));
    }

    [Fact(Timeout = 15000)]
    public async Task SingleKeyCommandsReuseTheShardSocketAndRouteLowercaseNames()
    {
        using var first = Listen();
        using var second = Listen();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync("127.0.0.1", first.Port);
        using TcpClient seed = await first.AcceptTcpClientAsync();
        NetworkStream seedStream = seed.GetStream();

        await ExpectAsync(seedStream, "CLUSTER", "SLOTS");
        await ReplyAsync(seedStream, SlotsReply(first.Port, second.Port));
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        ValueTask<long> incr = client.IncrementAsync("somekey");
        using TcpClient owner = await second.AcceptTcpClientAsync();
        NetworkStream ownerStream = owner.GetStream();

        await ExpectAsync(ownerStream, "INCR", "somekey");
        await ReplyAsync(ownerStream, ":1\r\n");
        Assert.Equal(1, await incr);

        ValueTask<RespValue> raw = client.ExecuteAsync(["get", "somekey"]);

        await ExpectAsync(ownerStream, "get", "somekey");
        await ReplyAsync(ownerStream, "$1\r\n1\r\n");
        Assert.Equal("1", (await raw).AsString());

        ValueTask<string?> seedKey = client.GetStringAsync("{user1000}.following");

        await ExpectAsync(seedStream, "GET", "{user1000}.following");
        await ReplyAsync(seedStream, "$1\r\nx\r\n");
        Assert.Equal("x", await seedKey);
    }

    [Fact(Timeout = 15000)]
    public async Task PipelineIsGroupedByShardAndKeepsCommandOrder()
    {
        using var first = Listen();
        using var second = Listen();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync("127.0.0.1", first.Port);
        using TcpClient seed = await first.AcceptTcpClientAsync();
        NetworkStream seedStream = seed.GetStream();

        await ExpectAsync(seedStream, "CLUSTER", "SLOTS");
        await ReplyAsync(seedStream, SlotsReply(first.Port, second.Port));
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        ValueTask<IReadOnlyList<RespValue>> pending = client.ExecuteManyAsync(
        [
            ["GET", "somekey"],
            ["GET", "{user1000}.following"],
            ["INCR", "somekey"],
        ]);
        Task<TcpClient> secondAccept = second.AcceptTcpClientAsync();

        await ExpectAsync(seedStream, "GET", "{user1000}.following");
        await ReplyAsync(seedStream, "$4\r\nleft\r\n");
        using TcpClient owner = await secondAccept;
        NetworkStream ownerStream = owner.GetStream();

        await ExpectAsync(ownerStream, "GET", "somekey");
        await ExpectAsync(ownerStream, "INCR", "somekey");
        await ReplyAsync(ownerStream, "$5\r\nright\r\n:2\r\n");
        IReadOnlyList<RespValue> replies = await pending;

        Assert.Equal("right", replies[0].AsString());
        Assert.Equal("left", replies[1].AsString());
        Assert.Equal(2, replies[2].Integer);
    }

    [Fact(Timeout = 15000)]
    public async Task ConnectRoutesBySlotAndOpensTheOwner()
    {
        using var first = Listen();
        using var second = Listen();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync("127.0.0.1", first.Port);
        using TcpClient seed = await first.AcceptTcpClientAsync();
        NetworkStream seedStream = seed.GetStream();

        await ExpectAsync(seedStream, "CLUSTER", "SLOTS");
        await ReplyAsync(seedStream, SlotsReply(first.Port, second.Port));
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        Assert.True(client.IsCluster);
        Assert.Equal(2, client.ShardCount);

        ValueTask<bool> set = client.SetAsync("somekey", "v");
        using TcpClient owner = await second.AcceptTcpClientAsync();
        NetworkStream ownerStream = owner.GetStream();

        await ExpectAsync(ownerStream, "SET", "somekey", "v");
        await ReplyAsync(ownerStream, "+OK\r\n");
        Assert.True(await set);
    }

    [Fact(Timeout = 15000)]
    public async Task MovedRefreshesTheMapAndRetriesOnTheNewOwner()
    {
        using var first = Listen();
        using var second = Listen();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync("127.0.0.1", first.Port);
        using TcpClient seed = await first.AcceptTcpClientAsync();
        NetworkStream seedStream = seed.GetStream();

        await ExpectAsync(seedStream, "CLUSTER", "SLOTS");
        await ReplyAsync(seedStream, OneNodeSlots(first.Port));
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        Assert.True(client.IsCluster);
        Assert.Equal(1, client.ShardCount);

        ValueTask<string?> get = client.GetStringAsync("somekey");

        await ExpectAsync(seedStream, "GET", "somekey");
        await ReplyAsync(seedStream, $"-MOVED 11058 127.0.0.1:{second.Port}\r\n");
        await ExpectAsync(seedStream, "CLUSTER", "SLOTS");
        await ReplyAsync(seedStream, SlotsReply(first.Port, second.Port));
        using TcpClient owner = await second.AcceptTcpClientAsync();
        NetworkStream ownerStream = owner.GetStream();

        await ExpectAsync(ownerStream, "GET", "somekey");
        await ReplyAsync(ownerStream, "$1\r\nv\r\n");
        Assert.Equal("v", await get);
    }

    [Fact(Timeout = 15000)]
    public async Task ExecuteBinaryAsyncRedirectsOnlyOnExplicitMOVEDAndKeepsPayloadBytes()
    {
        using var first = Listen();
        using var second = Listen();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync("127.0.0.1", first.Port);
        using TcpClient seed = await first.AcceptTcpClientAsync();
        NetworkStream seedStream = seed.GetStream();

        await ExpectAsync(seedStream, "CLUSTER", "SLOTS");
        await ReplyAsync(seedStream, OneNodeSlots(first.Port));
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        ReadOnlyMemory<byte>[] arguments =
        [
            "EVAL"u8.ToArray(), "return ruvio.call('SET',KEYS[1],ARGV[1])"u8.ToArray(),
            "1"u8.ToArray(), "somekey"u8.ToArray(), new byte[] { 255, 0, 128 },
        ];
        byte[] expected = RespCodec.Encode(arguments);
        ValueTask<RespValue> command = client.ExecuteBinaryAsync(arguments);
        var received = new byte[expected.Length];

        await TestDeadlines.ReadExactlyAsync(seedStream, received);
        Assert.Equal(expected, received);
        await ReplyAsync(seedStream, $"-MOVED 11058 127.0.0.1:{second.Port}\r\n");
        await ExpectAsync(seedStream, "CLUSTER", "SLOTS");
        await ReplyAsync(seedStream, SlotsReply(first.Port, second.Port));
        using TcpClient owner = await second.AcceptTcpClientAsync();

        await TestDeadlines.ReadExactlyAsync(owner.GetStream(), received);
        Assert.Equal(expected, received);
        await ReplyAsync(owner.GetStream(), "+OK\r\n");
        Assert.Equal("OK", (await command).AsString());

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await client.ExecuteBinaryAsync(new ReadOnlyMemory<byte>[] { "FLUSHDB"u8.ToArray() }));

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await client.ExecuteBinaryAsync(new ReadOnlyMemory<byte>[]
                { "EVAL"u8.ToArray(), "return 1"u8.ToArray(), "2"u8.ToArray(), "key"u8.ToArray() }));

        await Assert.ThrowsAsync<RuvioException>(async () =>
            await client.ExecuteBinaryAsync(new ReadOnlyMemory<byte>[]
                { "DEL"u8.ToArray(), "somekey"u8.ToArray(), "{user1000}.following"u8.ToArray() }));
    }

    [Fact(Timeout = 15000)]
    public async Task MgetIsSplitAcrossShardsAndMergedInKeyOrder()
    {
        using var first = Listen();
        using var second = Listen();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync("127.0.0.1", first.Port);
        using TcpClient seed = await first.AcceptTcpClientAsync();
        NetworkStream seedStream = seed.GetStream();

        await ExpectAsync(seedStream, "CLUSTER", "SLOTS");
        await ReplyAsync(seedStream, SlotsReply(first.Port, second.Port));
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        ValueTask<IReadOnlyList<string?>> pending = client.GetManyAsync("{user1000}.following", "somekey");
        Task<TcpClient> secondAccept = second.AcceptTcpClientAsync();

        await ExpectAsync(seedStream, "MGET", "{user1000}.following");
        await ReplyAsync(seedStream, "*1\r\n$4\r\nleft\r\n");
        using TcpClient owner = await secondAccept;
        NetworkStream ownerStream = owner.GetStream();

        await ExpectAsync(ownerStream, "MGET", "somekey");
        await ReplyAsync(ownerStream, "*1\r\n$5\r\nright\r\n");
        Assert.Equal(["left", "right"], await pending);
    }

    [Fact(Timeout = 15000)]
    public async Task MovedAfterAPlainConnectTurnsTheClientIntoARouter()
    {
        using var first = Listen();
        using var second = Listen();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = "127.0.0.1",
            Port = first.Port,
            DiscoverCluster = false,
        });

        using TcpClient seed = await first.AcceptTcpClientAsync();

        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        Assert.False(client.IsCluster);

        ValueTask<bool> set = client.SetAsync("somekey", "v");
        NetworkStream seedStream = seed.GetStream();

        await ExpectAsync(seedStream, "SET", "somekey", "v");
        await ReplyAsync(seedStream, $"-MOVED 11058 127.0.0.1:{second.Port}\r\n");
        await ExpectAsync(seedStream, "CLUSTER", "SLOTS");
        await ReplyAsync(seedStream, SlotsReply(first.Port, second.Port));
        using TcpClient owner = await second.AcceptTcpClientAsync();
        NetworkStream ownerStream = owner.GetStream();

        await ExpectAsync(ownerStream, "SET", "somekey", "v");
        await ReplyAsync(ownerStream, "+OK\r\n");
        Assert.True(await set);
        Assert.True(client.IsCluster);
        Assert.Equal(2, client.ShardCount);
    }

    [Fact(Timeout = 15000)]
    public async Task ClusterRejectsALogicalDatabase()
    {
        using var first = Listen();
        using var second = Listen();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = "127.0.0.1",
            Port = first.Port,
            Database = 2,
        });

        using TcpClient seed = await first.AcceptTcpClientAsync();

        await ReplyAsync(seed.GetStream(), SlotsReply(first.Port, second.Port));
        RuvioException error = await Assert.ThrowsAsync<RuvioException>(async () => await connecting);

        Assert.Contains("cluster mode", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact(Timeout = 15000)]
    public async Task FlushAndDbsizeGoToEveryShard()
    {
        using var first = Listen();
        using var second = Listen();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync("127.0.0.1", first.Port);
        using TcpClient seed = await first.AcceptTcpClientAsync();
        NetworkStream seedStream = seed.GetStream();

        await ExpectAsync(seedStream, "CLUSTER", "SLOTS");
        await ReplyAsync(seedStream, SlotsReply(first.Port, second.Port));
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        ValueTask flush = client.FlushDatabaseAsync();
        Task<TcpClient> secondAccept = second.AcceptTcpClientAsync();

        await ExpectAsync(seedStream, "FLUSHDB");
        await ReplyAsync(seedStream, "+OK\r\n");
        using TcpClient owner = await secondAccept;
        NetworkStream ownerStream = owner.GetStream();

        await ExpectAsync(ownerStream, "FLUSHDB");
        await ReplyAsync(ownerStream, "+OK\r\n");
        await flush;

        ValueTask<long> size = client.GetDatabaseSizeAsync();

        await ExpectAsync(seedStream, "DBSIZE");
        await ReplyAsync(seedStream, ":3\r\n");
        await ExpectAsync(ownerStream, "DBSIZE");
        await ReplyAsync(ownerStream, ":4\r\n");
        Assert.Equal(7, await size);
    }

    private static Listener Listen()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);

        listener.Start();

        return new Listener(listener);
    }

    private static async Task ExpectAsync(NetworkStream stream, params string[] arguments)
    {
        byte[] expected = RespCodec.Encode(arguments);
        var got = new byte[expected.Length];

        await TestDeadlines.ReadExactlyAsync(stream, got);
        Assert.Equal(Encoding.ASCII.GetString(expected), Encoding.ASCII.GetString(got));
    }

    private static Task ReplyAsync(NetworkStream stream, string resp)
    {
        return stream.WriteAsync(Encoding.ASCII.GetBytes(resp)).AsTask();
    }

    private static string SlotsReply(int port0, int port1)
    {
        return $"*2\r\n{SlotRange(0, 8191, port0, 1)}{SlotRange(8192, 16383, port1, 2)}";
    }

    private static string OneNodeSlots(int port)
    {
        return $"*1\r\n{SlotRange(0, 16383, port, 1)}";
    }

    private static string SlotRange(int start, int end, int port, int shard)
    {
        string id = shard.ToString("x40");

        return $"*3\r\n:{start}\r\n:{end}\r\n*3\r\n$9\r\n127.0.0.1\r\n:{port}\r\n${id.Length}\r\n{id}\r\n";
    }

    private sealed class Listener : IDisposable
    {
        private readonly TcpListener _listener;

        public Listener(TcpListener listener)
        {
            _listener = listener;
        }

        public int Port => ((IPEndPoint)_listener.LocalEndpoint!).Port;

        public Task<TcpClient> AcceptTcpClientAsync()
        {
            return TestDeadlines.AcceptAsync(_listener);
        }

        public void Dispose()
        {
            _listener.Stop();
        }
    }
}
