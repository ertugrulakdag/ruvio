using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

public class PubSubCommandTests
{
    [Fact(Timeout = 15000)]
    public async Task SubscribeUsesASecondSocketAndLeavesCommandsOnTheFirst()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        Assert.True(session.Client.IsConnected);

        ValueTask subscribe = session.Client.SubscribeAsync("feat-news", _ => { });

        await using SubscriberPipe subscriber = await Accept(session);
        await subscriber.Expect("SUBSCRIBE", "feat-news");
        await subscriber.Reply("*3\r\n$9\r\nsubscribe\r\n$9\r\nfeat-news\r\n:1\r\n");
        await subscribe;

        ValueTask<string> ping = session.Client.PingAsync();

        await session.Expect("PING");
        await session.Reply("+PONG\r\n");
        Assert.Equal("PONG", await ping);
    }

    [Fact(Timeout = 15000)]
    public async Task HandlerAndEventReceiveAPublishedPayload()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        var seen = new TaskCompletionSource<PubSubMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

        session.Client.Message += (_, message) => seen.TrySetResult(message);

        ValueTask subscribe = session.Client.SubscribeAsync("feat-news", message => seen.TrySetResult(message));

        await using SubscriberPipe subscriber = await Accept(session);
        await subscriber.Expect("SUBSCRIBE", "feat-news");
        await subscriber.Reply("*3\r\n$9\r\nsubscribe\r\n$9\r\nfeat-news\r\n:1\r\n");
        await subscribe;

        await subscriber.Reply("*3\r\n$7\r\nmessage\r\n$9\r\nfeat-news\r\n$11\r\nhello-ruvio\r\n");
        PubSubMessage message = await seen.Task.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Equal("feat-news", message.Channel);
        Assert.Equal("hello-ruvio", message.Payload);
    }

    [Fact(Timeout = 15000)]
    public async Task ReadPubSubMessageReadsFromTheSubscriberSocket()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<IReadOnlyList<RespValue>> sub = session.Client.SubscribeAsync("feat-news");

        await using SubscriberPipe subscriber = await Accept(session);
        await subscriber.Expect("SUBSCRIBE", "feat-news");
        await subscriber.Reply("*3\r\n$9\r\nsubscribe\r\n$9\r\nfeat-news\r\n:1\r\n");
        Assert.Equal("subscribe", (await sub)[0].Items![0].AsString());

        ValueTask<PubSubMessage> pending = session.Client.ReadPubSubMessageAsync();

        await subscriber.Reply("*3\r\n$7\r\nmessage\r\n$9\r\nfeat-news\r\n$11\r\nhello-ruvio\r\n");
        PubSubMessage message = await pending;

        Assert.Equal("message", message.Kind);
        Assert.Equal("feat-news", message.Channel);
        Assert.Equal("hello-ruvio", message.Payload);
    }

    [Fact(Timeout = 15000)]
    public async Task ListenAsyncYieldsDeliveries()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<IReadOnlyList<RespValue>> subscribe = session.Client.SubscribeAsync("news");

        await using SubscriberPipe subscriber = await Accept(session);
        await subscriber.Expect("SUBSCRIBE", "news");
        await subscriber.Reply("*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n");
        await subscribe;

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        IAsyncEnumerator<PubSubMessage> listen = session.Client.ListenAsync(timeout.Token).GetAsyncEnumerator();
        ValueTask<bool> move = listen.MoveNextAsync();

        await subscriber.Reply("*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$2\r\nhi\r\n");
        Assert.True(await move);
        Assert.Equal("hi", listen.Current.Payload);
        await listen.DisposeAsync();
    }

    [Fact(Timeout = 15000)]
    public async Task PatternAndShardedSubscribeUseTheSubscriberSocket()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        var seen = new TaskCompletionSource<PubSubMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        ValueTask pattern = session.Client.SubscribePatternsAsync("n*", seen.SetResult);

        await using SubscriberPipe subscriber = await Accept(session);
        await subscriber.Expect("PSUBSCRIBE", "n*");
        await subscriber.Reply("*3\r\n$10\r\npsubscribe\r\n$2\r\nn*\r\n:1\r\n");
        await pattern;
        await subscriber.Reply("*4\r\n$8\r\npmessage\r\n$2\r\nn*\r\n$4\r\nnews\r\n$2\r\nhi\r\n");
        Assert.Equal("hi", (await seen.Task.WaitAsync(TimeSpan.FromSeconds(2))).Payload);
    }

    [Fact(Timeout = 15000)]
    public async Task PingReadsAPubsubPongArray()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<string> ping = session.Client.PingAsync();

        await session.Expect("PING");
        await session.Reply("*2\r\n$4\r\npong\r\n$0\r\n\r\n");
        Assert.Equal("", await ping);

        ValueTask<string> echoed = session.Client.PingAsync("hello");

        await session.Expect("PING", "hello");
        await session.Reply("*2\r\n$4\r\npong\r\n$5\r\nhello\r\n");
        Assert.Equal("hello", await echoed);
    }

    [Fact(Timeout = 15000)]
    public async Task UnsubscribeWithoutNamesUsesTheSubscriberSocket()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<IReadOnlyList<RespValue>> unsub = session.Client.UnsubscribeAsync();

        await using SubscriberPipe subscriber = await Accept(session);
        await subscriber.Expect("UNSUBSCRIBE");
        await subscriber.Reply("*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n");
        Assert.Equal(0, (await unsub)[0].Items![2].Integer);
    }

    private static async Task<SubscriberPipe> Accept(ScriptedSession session)
    {
        return await session.AcceptSubscriberAsync();
    }
}
