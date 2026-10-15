using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

public class PubSubHandlerTests
{
    [Fact(Timeout = 15000)]
    public async Task TwoHandlersOnOneChannelBothRun()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        var first = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        ValueTask a = session.Client.SubscribeAsync("news", message => first.TrySetResult(message.Payload));

        await using SubscriberPipe subscriber = await session.AcceptSubscriberAsync();
        await Confirm(subscriber, "SUBSCRIBE", "news", a);
        ValueTask extra = session.Client.SubscribeAsync("news", message => second.TrySetResult(message.Payload));

        await subscriber.Expect("SUBSCRIBE", "news");
        await subscriber.Reply("*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:2\r\n");
        await extra;
        await Push(subscriber, "news", "hi");
        Assert.Equal("hi", await first.Task.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.Equal("hi", await second.Task.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    [Fact(Timeout = 15000)]
    public async Task HandlerForAnotherChannelDoesNotRun()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        var wrong = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var right = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        ValueTask subscribe = session.Client.SubscribeAsync("keep", message => right.TrySetResult(message.Payload));

        await using SubscriberPipe subscriber = await session.AcceptSubscriberAsync();
        await Confirm(subscriber, "SUBSCRIBE", "keep", subscribe);
        session.Client.RegisterHandler("other", message => wrong.TrySetResult(message.Payload));
        await Push(subscriber, "keep", "only");
        Assert.Equal("only", await right.Task.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.False(wrong.Task.IsCompleted);
    }

    [Fact(Timeout = 15000)]
    public async Task AsyncHandlerReceivesThePayload()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        var seen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        ValueTask subscribe = session.Client.SubscribeAsync("news", async message =>
        {
            await Task.Yield();
            seen.TrySetResult(message.Payload);
        });

        await using SubscriberPipe subscriber = await session.AcceptSubscriberAsync();
        await Confirm(subscriber, "SUBSCRIBE", "news", subscribe);
        await Push(subscriber, "news", "later");
        Assert.Equal("later", await seen.Task.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    [Fact(Timeout = 15000)]
    public async Task UnsubscribeStopsTheChannelHandler()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        int count = 0;
        ValueTask subscribe = session.Client.SubscribeAsync("news", _ => Interlocked.Increment(ref count));

        await using SubscriberPipe subscriber = await session.AcceptSubscriberAsync();
        await Confirm(subscriber, "SUBSCRIBE", "news", subscribe);
        ValueTask<IReadOnlyList<RespValue>> unsub = session.Client.UnsubscribeAsync("news");

        await subscriber.Expect("UNSUBSCRIBE", "news");
        await subscriber.Reply("*3\r\n$11\r\nunsubscribe\r\n$4\r\nnews\r\n:0\r\n");
        await unsub;
        await Push(subscriber, "news", "ignored");
        await Task.Delay(50);
        Assert.Equal(0, count);
    }

    [Fact(Timeout = 15000)]
    public async Task TwoChannelsRouteToTheMatchingHandler()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        var a = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var b = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        ValueTask first = session.Client.SubscribeAsync("a", message => a.TrySetResult(message.Payload));

        await using SubscriberPipe subscriber = await session.AcceptSubscriberAsync();
        await Confirm(subscriber, "SUBSCRIBE", "a", first);
        ValueTask second = session.Client.SubscribeAsync("b", message => b.TrySetResult(message.Payload));

        await subscriber.Expect("SUBSCRIBE", "b");
        await subscriber.Reply("*3\r\n$9\r\nsubscribe\r\n$1\r\nb\r\n:2\r\n");
        await second;
        await Push(subscriber, "b", "bee");
        await Push(subscriber, "a", "aye");
        Assert.Equal("bee", await b.Task.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.Equal("aye", await a.Task.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    [Fact(Timeout = 15000)]
    public async Task ShardedHandlerReadsSmessage()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        var seen = new TaskCompletionSource<PubSubMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        ValueTask subscribe = session.Client.SubscribeShardedAsync("slot", seen.SetResult);

        await using SubscriberPipe subscriber = await session.AcceptSubscriberAsync();
        await subscriber.Expect("SSUBSCRIBE", "slot");
        await subscriber.Reply("*3\r\n$10\r\nssubscribe\r\n$4\r\nslot\r\n:1\r\n");
        await subscribe;
        await subscriber.Reply("*3\r\n$8\r\nsmessage\r\n$4\r\nslot\r\n$3\r\nxyz\r\n");
        PubSubMessage message = await seen.Task.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Equal("smessage", message.Kind);
        Assert.Equal("xyz", message.Payload);
    }

    [Fact(Timeout = 15000)]
    public async Task KeyEventHelpersSubscribeToEventsForTheSelectedDatabase()
    {
        await using ScriptedSession session = await ScriptedSession.Open(3);
        Assert.Equal(3, session.Client.Database);

        var expired = new TaskCompletionSource<PubSubMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        ValueTask subscribeExpired = session.Client.SubscribeExpiredAsync(expired.SetResult);
        await using SubscriberPipe subscriber = await session.AcceptSubscriberAsync();
        await subscriber.Expect("SSUBSCRIBE", "__keyevent@3__:expired");
        await subscriber.Reply("*3\r\n$10\r\nssubscribe\r\n$22\r\n__keyevent@3__:expired\r\n:1\r\n");
        await subscribeExpired;
        await subscriber.Reply("*3\r\n$8\r\nsmessage\r\n$22\r\n__keyevent@3__:expired\r\n$3\r\nkey\r\n");

        Assert.Equal("key", (await expired.Task.WaitAsync(TimeSpan.FromSeconds(2))).Payload);

        var evicted = new TaskCompletionSource<PubSubMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        ValueTask subscribeEvicted = session.Client.SubscribeEvictedAsync(evicted.SetResult);
        await subscriber.Expect("SSUBSCRIBE", "__keyevent@3__:evicted");
        await subscriber.Reply("*3\r\n$10\r\nssubscribe\r\n$22\r\n__keyevent@3__:evicted\r\n:2\r\n");
        await subscribeEvicted;
        await subscriber.Reply("*3\r\n$8\r\nsmessage\r\n$22\r\n__keyevent@3__:evicted\r\n$3\r\nkey\r\n");

        Assert.Equal("key", (await evicted.Task.WaitAsync(TimeSpan.FromSeconds(2))).Payload);
    }

    [Fact(Timeout = 15000)]
    public async Task ConnectionFailedFiresWhenTheSubscriberSocketCloses()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        var failed = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);

        session.Connection.ConnectionFailed += (_, error) => failed.TrySetResult(error);
        ValueTask subscribe = session.Client.SubscribeAsync("news", _ => { });

        await using SubscriberPipe subscriber = await session.AcceptSubscriberAsync();
        await Confirm(subscriber, "SUBSCRIBE", "news", subscribe);
        subscriber.Close();
        Assert.NotNull(await failed.Task.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    [Fact(Timeout = 15000)]
    public async Task EmptyPayloadAndUtf8PayloadAreDelivered()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        var seen = new ChannelBox();
        ValueTask subscribe = session.Client.SubscribeAsync("news", message => seen.Add(message.Payload));

        await using SubscriberPipe subscriber = await session.AcceptSubscriberAsync();
        await Confirm(subscriber, "SUBSCRIBE", "news", subscribe);
        await subscriber.Reply("*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$0\r\n\r\n");
        await subscriber.Reply("*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$2\r\nOK\r\n");
        Assert.Equal("", await seen.Next());
        Assert.Equal("OK", await seen.Next());
    }

    private static async Task Confirm(SubscriberPipe subscriber, string command, string name, ValueTask pending)
    {
        await subscriber.Expect(command, name);
        string kind = command.ToLowerInvariant();

        await subscriber.Reply($"*3\r\n${kind.Length}\r\n{kind}\r\n${name.Length}\r\n{name}\r\n:1\r\n");
        await pending;
    }

    private static Task Push(SubscriberPipe subscriber, string channel, string payload)
    {
        return subscriber.Reply(
            $"*3\r\n$7\r\nmessage\r\n${channel.Length}\r\n{channel}\r\n${payload.Length}\r\n{payload}\r\n");
    }

    private sealed class ChannelBox
    {
        private readonly Queue<string> _values = new();
        private readonly Queue<TaskCompletionSource<string>> _waiters = new();

        public void Add(string value)
        {
            lock (this)
            {
                if (_waiters.Count > 0)
                {
                    _waiters.Dequeue().TrySetResult(value);

                    return;
                }

                _values.Enqueue(value);
            }
        }

        public Task<string> Next()
        {
            lock (this)
            {
                if (_values.Count > 0)
                {
                    return Task.FromResult(_values.Dequeue());
                }

                var waiter = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

                _waiters.Enqueue(waiter);

                return waiter.Task.WaitAsync(TimeSpan.FromSeconds(2));
            }
        }
    }
}
