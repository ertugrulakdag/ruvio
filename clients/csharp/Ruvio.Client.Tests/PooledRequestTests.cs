using Ruvio.Client;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>
/// Requests are pooled <see cref="System.Threading.Tasks.Sources.IValueTaskSource"/> objects.
/// These tests pin the invariants that keep a recycled request from leaking another caller's reply.
/// </summary>
public class PooledRequestTests
{
    [Theory(Timeout = 30000)]
    [InlineData(1, 8, false)]
    [InlineData(1, 8, true)]
    [InlineData(16, 8, false)]
    [InlineData(16, 8, true)]
    [InlineData(128, 8, false)]
    [InlineData(128, 8, true)]
    [InlineData(128, 256, false)]
    [InlineData(128, 256, true)]
    public async Task RepeatedInlineAndListCommandsPreserveRepliesAcrossWriterHandoffs(
        int workers, int capacity, bool inline)
    {
        await using EchoServer server = await EchoServer.StartAsync();
        using RuvioConnection connection = await server.ConnectAsync(capacity);
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(25));
        int completed = 0;

        await Task.WhenAll(Enumerable.Range(0, workers).Select(worker => Task.Run(async () =>
        {
            for (int iteration = 0; iteration < 512; iteration++)
            {
                string expected = $"{worker}:{iteration}";
                RespValue reply = inline
                    ? await client.ExecuteCommandAsync(new RespCommand("GET", expected), CancellationToken.None)
                    : await client.ExecuteAsync(["GET", expected]);
                Assert.Equal(expected, reply.AsString());
                Interlocked.Increment(ref completed);
            }
        }))).WaitAsync(deadline.Token);

        Assert.Equal(workers * 512, completed);
        Assert.Equal("after", await client.GetStringAsync("after", deadline.Token));
    }

    [Fact(Timeout = 30000)]
    public async Task CancelledCallersNeverObserveAnotherCallersReplyUnderSaturation()
    {
        // Four in-flight slots force most callers through the capacity-wait path while a third
        // of them cancel at random points: before publish, while queued, or while awaiting a reply.
        await using EchoServer server = await EchoServer.StartAsync();
        using RuvioConnection connection = await server.ConnectAsync(maxInFlight: 4);
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        int completed = 0, cancelled = 0;

        await Task.WhenAll(Enumerable.Range(0, 32).Select(worker => Task.Run(async () =>
        {
            var random = new Random(worker);
            for (int i = 0; i < 200; i++)
            {
                string value = $"{worker}:{i}";
                using var cancel = new CancellationTokenSource();
                bool cancels = i % 3 == 0;
                if (cancels)
                {
                    if (random.Next(4) == 0) cancel.Cancel();
                    else cancel.CancelAfter(TimeSpan.FromTicks(random.Next(1, 20_000)));
                }

                try
                {
                    RespValue reply = await client.ExecuteAsync(["ECHO", value], cancel.Token);
                    Assert.Equal(value, reply.AsString());
                    Interlocked.Increment(ref completed);
                }
                catch (OperationCanceledException) when (cancels)
                {
                    Interlocked.Increment(ref cancelled);
                }
            }
        })));

        Assert.Equal(32 * 200, completed + cancelled);
        // Only callers that never cancel are guaranteed to complete; a slow host may cancel every other one.
        Assert.True(completed >= 32 * (200 - (200 + 2) / 3));
        Assert.Equal("after", (await client.ExecuteAsync("ECHO", "after")).AsString());
    }

    [Fact(Timeout = 15000)]
    public async Task AwaitingAMultiplexerRequestTwiceThrowsWithoutRecyclingItEarly()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var tcp = new TcpClient { NoDelay = true };
        Task connecting = tcp.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port);
        using TcpClient server = await TestDeadlines.AcceptAsync(listener);
        await connecting;
        var mux = new CommandMultiplexer(tcp.GetStream(), new RespReader(tcp.GetStream()), 4);
        try
        {
            using var cancel = new CancellationTokenSource();
            ValueTask<RespValue> abandoned = mux.SendCommandAsync(new RespCommand("ECHO", "abandoned"), cancel.Token);
            cancel.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await abandoned);

            // The reply is still owed, so the request is still owned by the pipeline here.
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await abandoned);

            ValueTask<RespValue> next = mux.SendCommandAsync(new RespCommand("ECHO", "next"), CancellationToken.None);
            await server.GetStream().WriteAsync("+abandoned\r\n+next\r\n"u8.ToArray());
            Assert.Equal("next", (await next).AsString());

            ValueTask<RespValue> done = mux.SendCommandAsync(new RespCommand("ECHO", "done"), CancellationToken.None);
            await server.GetStream().WriteAsync("+done\r\n"u8.ToArray());
            Assert.Equal("done", (await done).AsString());
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await done);
        }
        finally
        {
            await mux.StopAsync();
        }
    }

    [Fact(Timeout = 15000)]
    public async Task ValueTasksComposeWithWhenAllThroughAsTask()
    {
        await using EchoServer server = await EchoServer.StartAsync();
        using RuvioConnection connection = await server.ConnectAsync(maxInFlight: 2);
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        RespValue[] replies = await Task.WhenAll(Enumerable.Range(0, 64)
            .Select(i => client.ExecuteAsync("ECHO", i.ToString()).AsTask()));

        Assert.Equal(Enumerable.Range(0, 64).Select(i => i.ToString()), replies.Select(reply => reply.AsString()));
    }

    /// <summary>Answers every command with its last argument as a bulk string.</summary>
    private sealed class EchoServer : IAsyncDisposable
    {
        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _stop = new();
        private readonly List<Task> _sessions = [];
        private Task _accepting = Task.CompletedTask;

        private EchoServer(TcpListener listener) => _listener = listener;

        public static Task<EchoServer> StartAsync()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var server = new EchoServer(listener);
            server._accepting = server.AcceptAsync();
            return Task.FromResult(server);
        }

        public Task<RuvioConnection> ConnectAsync(int maxInFlight) =>
            RuvioConnection.ConnectAsync(new RuvioClientOptions
            {
                Port = ((IPEndPoint)_listener.LocalEndpoint).Port,
                DiscoverCluster = false,
                Reconnect = false,
                MaxInFlightRequests = maxInFlight,
            });

        private async Task AcceptAsync()
        {
            try
            {
                while (true)
                {
                    TcpClient client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    lock (_sessions) _sessions.Add(ServeAsync(client));
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        private async Task ServeAsync(TcpClient client)
        {
            using (client)
            {
                NetworkStream stream = client.GetStream();
                using var reader = new RespReader(stream);
                try
                {
                    while (true)
                    {
                        RespValue command = await reader.ReadAsync(_stop.Token);
                        IReadOnlyList<RespValue> items = command.Items!;
                        string value = items[items.Count - 1].AsString()!;
                        await stream.WriteAsync(Encoding.UTF8.GetBytes($"${Encoding.UTF8.GetByteCount(value)}\r\n{value}\r\n"), _stop.Token);
                    }
                }
                catch (Exception exception) when (exception is OperationCanceledException or IOException or RuvioException)
                {
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            _stop.Cancel();
            _listener.Stop();
            await _accepting;
            Task[] sessions;
            lock (_sessions) sessions = [.. _sessions];
            await Task.WhenAll(sessions);
            _stop.Dispose();
        }
    }
}
