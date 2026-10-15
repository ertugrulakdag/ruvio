using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

public class PubSubRecoveryTests
{
    private static string Confirm(string kind, string name, int count) =>
        $"*3\r\n${kind.Length}\r\n{kind}\r\n${name.Length}\r\n{name}\r\n:{count}\r\n";

    private static string Push(string channel, string payload) =>
        $"*3\r\n$7\r\nmessage\r\n${channel.Length}\r\n{channel}\r\n${payload.Length}\r\n{payload}\r\n";

    private static async Task<(RuvioConnection Connection, RuvioClient Client, FakeConn Command)> Open(
        FakeServer server, Action<RuvioClientOptions>? configure = null)
    {
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(server.Options(configure));
        FakeConn command = await server.AcceptAsync();
        RuvioConnection connection = await connecting;

        return (connection, await connection.GetDefaultDatabaseAsync(), command);
    }

    [Fact(Timeout = 20000)]
    public async Task ADroppedSubscriberIsReopenedAndItsChannelsAreSubscribedAgain()
    {
        using var server = new FakeServer();
        (RuvioConnection connection, RuvioClient client, FakeConn command) = await Open(server, o =>
            o.ReconnectDelay = _ => TimeSpan.FromMilliseconds(10));
        using FakeConn _ = command;
        await using RuvioConnection _c = connection;
        var seen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var lost = new TaskCompletionSource<RuvioConnectionNotice>(TaskCreationOptions.RunContinuationsAsynchronously);
        var restored = new TaskCompletionSource<RuvioConnectionNotice>(TaskCreationOptions.RunContinuationsAsynchronously);

        connection.ConnectionLost += (_, notice) => { if (notice.Subscriber) { lost.TrySetResult(notice); } };
        connection.ConnectionRestored += (_, notice) => { if (notice.Subscriber) { restored.TrySetResult(notice); } };

        ValueTask subscribe = client.SubscribeAsync("news", message => seen.TrySetResult(message.Payload));
        using FakeConn first = await server.AcceptAsync();

        await first.Expect("SUBSCRIBE", "news");
        await first.Reply(Confirm("subscribe", "news", 1));
        await subscribe;
        ValueTask<IReadOnlyList<RespValue>> pattern = client.SubscribePatternsAsync("p*");

        await first.Expect("PSUBSCRIBE", "p*");
        await first.Reply(Confirm("psubscribe", "p*", 2));
        await pattern;

        first.Close();
        RuvioConnectionNotice drop = await lost.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.True(drop.Subscriber);
        Assert.NotNull(drop.Error);
        Assert.Equal(server.Port, drop.Port);

        using FakeConn second = await server.AcceptAsync();

        await second.Expect("SUBSCRIBE", "news");
        await second.Reply(Confirm("subscribe", "news", 1));
        await second.Expect("PSUBSCRIBE", "p*");
        await second.Reply(Confirm("psubscribe", "p*", 2));
        RuvioConnectionNotice back = await restored.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal("127.0.0.1", back.Host);
        Assert.Null(back.Error);
        await second.Reply(Push("news", "again"));
        Assert.Equal("again", await seen.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact(Timeout = 20000)]
    public async Task ASubscriberThatCannotBeReopenedReportsTheFailure()
    {
        using var server = new FakeServer();
        (RuvioConnection connection, RuvioClient client, FakeConn command) = await Open(server, o =>
            o.ReconnectDelay = _ => null);
        using FakeConn _ = command;
        await using RuvioConnection _c = connection;
        int losses = 0;
        var twice = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        connection.ConnectionLost += (_, notice) =>
        {
            if (notice.Subscriber && Interlocked.Increment(ref losses) == 2)
            {
                twice.TrySetResult();
            }
        };

        ValueTask subscribe = client.SubscribeAsync("news", _ => { });
        using FakeConn first = await server.AcceptAsync();

        await first.Expect("SUBSCRIBE", "news");
        await first.Reply(Confirm("subscribe", "news", 1));
        await subscribe;
        server.StopListening();
        first.Close();
        await twice.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact(Timeout = 20000)]
    public async Task WithoutReconnectADroppedSubscriberEndsTheListenStream()
    {
        using var server = new FakeServer();
        (RuvioConnection connection, RuvioClient client, FakeConn command) = await Open(server, o => o.Reconnect = false);
        using FakeConn _ = command;
        await using RuvioConnection _c = connection;

        ValueTask subscribe = client.SubscribeAsync("news", _ => { });
        using FakeConn first = await server.AcceptAsync();

        await first.Expect("SUBSCRIBE", "news");
        await first.Reply(Confirm("subscribe", "news", 1));
        await subscribe;

        ValueTask<PubSubMessage> read = client.ReadPubSubMessageAsync();

        first.Close();
        await Assert.ThrowsAnyAsync<Exception>(async () => await read);
    }

    [Fact(Timeout = 20000)]
    public async Task SubscribeErrorsPushesAndRawReadsAreSurfaced()
    {
        using var server = new FakeServer();
        (RuvioConnection connection, RuvioClient client, FakeConn command) = await Open(server);
        using FakeConn _ = command;
        await using RuvioConnection _c = connection;

        ValueTask denied = client.SubscribeAsync("secret", _ => { });
        using FakeConn pipe = await server.AcceptAsync();

        await pipe.Expect("SUBSCRIBE", "secret");
        await pipe.Reply("-NOPERM no access\r\n");
        RuvioException error = await Assert.ThrowsAsync<RuvioException>(async () => await denied);
        Assert.True(error.IsServerReply);

        ValueTask<IReadOnlyList<RespValue>> ok = client.SubscribeAsync("news");

        await pipe.Expect("SUBSCRIBE", "news");
        await pipe.Reply(Confirm("subscribe", "news", 1));
        await ok;

        ValueTask<RespValue> raw = client.ReadMessageAsync();

        await pipe.Reply(Push("news", "x"));
        RespValue push = await raw;
        Assert.Equal("message", push.Items![0].AsString());

        // A pattern delivery is rebuilt with its pattern for raw reads.
        ValueTask<IReadOnlyList<RespValue>> patterns = client.SubscribePatternsAsync("n*");

        await pipe.Expect("PSUBSCRIBE", "n*");
        await pipe.Reply(Confirm("psubscribe", "n*", 2));
        await patterns;
        ValueTask<RespValue> rawPattern = client.ReadMessageAsync();

        await pipe.Reply("*4\r\n$8\r\npmessage\r\n$2\r\nn*\r\n$4\r\nnews\r\n$2\r\nhi\r\n");
        RespValue pushed = await rawPattern;
        Assert.Equal(4, pushed.Items!.Count);
        Assert.Equal("n*", pushed.Items[1].AsString());
    }

    [Fact(Timeout = 20000)]
    public async Task AsyncHandlerOverloadsForPatternsAndShardsAndKeyEventsSubscribe()
    {
        using var server = new FakeServer();
        (RuvioConnection connection, RuvioClient client, FakeConn command) = await Open(server);
        using FakeConn _ = command;
        await using RuvioConnection _c = connection;
        var hits = new List<string>();

        Task Handler(PubSubMessage message)
        {
            lock (hits)
            {
                hits.Add(message.Payload);
            }

            return Task.CompletedTask;
        }

        ValueTask pattern = client.SubscribePatternsAsync("a*", Handler);
        using FakeConn pipe = await server.AcceptAsync();

        await pipe.Expect("PSUBSCRIBE", "a*");
        await pipe.Reply(Confirm("psubscribe", "a*", 1));
        await pattern;
        ValueTask sharded = client.SubscribeShardedAsync("s1", Handler);

        await pipe.Expect("SSUBSCRIBE", "s1");
        await pipe.Reply(Confirm("ssubscribe", "s1", 2));
        await sharded;
        ValueTask expired = client.SubscribeExpiredAsync(Handler);

        await pipe.Expect("SSUBSCRIBE", "__keyevent@0__:expired");
        await pipe.Reply(Confirm("ssubscribe", "__keyevent@0__:expired", 3));
        await expired;
        ValueTask evicted = client.SubscribeEvictedAsync(Handler);

        await pipe.Expect("SSUBSCRIBE", "__keyevent@0__:evicted");
        await pipe.Reply(Confirm("ssubscribe", "__keyevent@0__:evicted", 4));
        await evicted;
        ValueTask shardAction = client.SubscribeShardedAsync("s2", _ => { });

        await pipe.Expect("SSUBSCRIBE", "s2");
        await pipe.Reply(Confirm("ssubscribe", "s2", 5));
        await shardAction;
        ValueTask expiredAction = client.SubscribeExpiredAsync(_ => { });

        await pipe.Expect("SSUBSCRIBE", "__keyevent@0__:expired");
        await pipe.Reply(Confirm("ssubscribe", "__keyevent@0__:expired", 6));
        await expiredAction;
        ValueTask evictedAction = client.SubscribeEvictedAsync(_ => { });

        await pipe.Expect("SSUBSCRIBE", "__keyevent@0__:evicted");
        await pipe.Reply(Confirm("ssubscribe", "__keyevent@0__:evicted", 7));
        await evictedAction;

        await pipe.Reply("*4\r\n$8\r\npmessage\r\n$2\r\na*\r\n$3\r\nabc\r\n$1\r\nP\r\n");
        await pipe.Reply("*3\r\n$8\r\nsmessage\r\n$2\r\ns1\r\n$1\r\nS\r\n");
        await pipe.Reply("*3\r\n$8\r\nsmessage\r\n$22\r\n__keyevent@0__:expired\r\n$1\r\nE\r\n");

        for (int i = 0; i < 100 && hits.Count < 4; i++)
        {
            await Task.Delay(20);
        }

        lock (hits)
        {
            Assert.Contains("P", hits);
            Assert.Contains("S", hits);
            Assert.Contains("E", hits);
        }
    }

    [Fact(Timeout = 20000)]
    public async Task SubscribeWithNoChannelsIsRejectedAndCommandsStayUsable()
    {
        using var server = new FakeServer();
        (RuvioConnection connection, RuvioClient client, FakeConn command) = await Open(server);
        using FakeConn _ = command;
        await using RuvioConnection _c = connection;

        await Assert.ThrowsAnyAsync<Exception>(async () => await client.SubscribeAsync());
        await Assert.ThrowsAnyAsync<Exception>(async () => await client.SubscribePatternsAsync());
        ValueTask<string> ping = client.PingAsync();

        await command.Expect("PING");
        await command.Reply("+PONG\r\n");
        Assert.Equal("PONG", await ping);
    }
}
