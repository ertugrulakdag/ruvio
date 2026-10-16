using System.Net;
using System.Net.Sockets;
using System.Text;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

public class ReconnectTests
{
    [Fact(Timeout = 15000)]
    public async Task NextCommandAfterADropOpensANewSocket()
    {
        await using var server = new DropServer();
        await using RuvioConnection connection = await server.Connect();
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        server.CloseAccepted();
        await Assert.ThrowsAnyAsync<RuvioException>(async () => await client.PingAsync());
        ValueTask<string> again = client.PingAsync();

        await server.AcceptAndPong();
        Assert.Equal("PONG", await again);
        Assert.True(connection.IsConnected);
    }

    [Fact(Timeout = 15000)]
    public async Task ClosedGetIsNotSentAgainButTheNextCommandReconnects()
    {
        await using var server = new DropServer();
        await using RuvioConnection connection = await server.Connect();
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        server.CloseAccepted();

        // Before, this read was sent once more on a new socket and would wait here for a reply.
        RuvioException error = await Assert.ThrowsAsync<RuvioException>(async () => await client.GetStringAsync("k"));

        Assert.True(error.ClosedBeforeReply);
        ValueTask<string?> again = client.GetStringAsync("k");

        await server.AcceptAndNullGet();
        Assert.Null(await again);
        Assert.True(connection.IsConnected);
    }

    [Fact(Timeout = 15000)]
    public async Task ClosedSetIsNotSentAgain()
    {
        await using var server = new DropServer();
        int reconnects = 0;

        await using RuvioConnection connection = await server.Connect(options =>
        {
            options.ReconnectDelay = _ =>
            {
                reconnects++;

                return TimeSpan.Zero;
            };
        });
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        server.CloseAccepted();
        server.StopListening();
        await Assert.ThrowsAnyAsync<RuvioException>(async () => await client.SetAsync("k", "v"));
        Assert.Equal(0, reconnects);
    }

    [Fact(Timeout = 15000)]
    public async Task APipelineIsNotSentAgain()
    {
        await using var server = new DropServer();
        int reconnects = 0;

        await using RuvioConnection connection = await server.Connect(options =>
        {
            options.ReconnectDelay = _ =>
            {
                reconnects++;

                return TimeSpan.Zero;
            };
        });
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        server.CloseAccepted();
        server.StopListening();
        await Assert.ThrowsAnyAsync<RuvioException>(async () =>
            await client.ExecuteManyAsync([["GET", "k"], ["GET", "k"]]));
        Assert.Equal(0, reconnects);
    }

    [Fact(Timeout = 15000)]
    public async Task ExecuteBinaryAsyncDoesNotRetryAnAmbiguousMutation()
    {
        await using var server = new DropServer();
        int reconnects = 0;

        await using RuvioConnection connection = await server.Connect(options =>
        {
            options.ReconnectDelay = _ =>
            {
                reconnects++;

                return TimeSpan.Zero;
            };
        });
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        server.CloseAccepted();
        server.StopListening();
        await Assert.ThrowsAnyAsync<RuvioException>(async () =>
            await client.ExecuteBinaryAsync(new ReadOnlyMemory<byte>[]
            {
                "EVAL"u8.ToArray(), "return ruvio.call('DEL',KEYS[1])"u8.ToArray(),
                "1"u8.ToArray(), "key"u8.ToArray(),
            }));

        Assert.Equal(0, reconnects);
    }

    [Fact(Timeout = 15000)]
    public async Task GetThatAlreadyStartedAReplyIsNotSentAgain()
    {
        await using var server = new DropServer();
        await using RuvioConnection connection = await server.Connect();
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        ValueTask<string?> get = client.GetStringAsync("k");

        await server.CutAcceptedGetAsync();
        RuvioException error = await Assert.ThrowsAsync<RuvioException>(async () => await get);

        Assert.Equal("connection closed", error.Message);
        Assert.False(error.ClosedBeforeReply);
    }

    [Fact(Timeout = 15000)]
    public async Task ReconnectFalseLeavesTheRetryToTheCaller()
    {
        await using var server = new DropServer();
        await using RuvioConnection connection = await server.Connect(reconnect: false);
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        var lost = new TaskCompletionSource<RuvioConnectionNotice>(TaskCreationOptions.RunContinuationsAsynchronously);
        var restored = new TaskCompletionSource<RuvioConnectionNotice>(TaskCreationOptions.RunContinuationsAsynchronously);

        connection.ConnectionLost += (_, notice) => lost.TrySetResult(notice);
        connection.ConnectionRestored += (_, notice) => restored.TrySetResult(notice);
        server.CloseAccepted();
        await Assert.ThrowsAnyAsync<RuvioException>(async () => await client.PingAsync());
        await Assert.ThrowsAnyAsync<RuvioException>(async () => await client.PingAsync());
        Assert.False(lost.Task.IsCompleted);
        Task reconnecting = connection.ReconnectAsync();

        await server.AcceptQuietly();
        await reconnecting;
        RuvioConnectionNotice notice = await restored.Task.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.False(notice.Subscriber);
        ValueTask<string> ping = client.PingAsync();

        await server.Pong();
        Assert.Equal("PONG", await ping);
    }

    [Fact(Timeout = 15000)]
    public async Task CustomDelayCanStopTheRetry()
    {
        await using var server = new DropServer();
        int asks = 0;

        await using RuvioConnection connection = await server.Connect(options =>
        {
            options.MaxReconnectAttempts = 8;
            options.ReconnectDelay = _ =>
            {
                asks++;

                return null;
            };
        });
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        server.CloseAccepted();
        server.StopListening();
        await Assert.ThrowsAnyAsync<Exception>(async () => await client.PingAsync());
        await Assert.ThrowsAnyAsync<Exception>(async () => await client.PingAsync());
        Assert.Equal(1, asks);
    }

    private sealed class DropServer : IAsyncDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private TcpClient? _accepted;

        public DropServer()
        {
            _listener.Start();
        }

        public async Task<RuvioConnection> Connect(bool reconnect = true)
        {
            return await Connect(options => options.Reconnect = reconnect);
        }

        public async Task<RuvioConnection> Connect(Action<RuvioClientOptions> configure)
        {
            int port = ((IPEndPoint)_listener.LocalEndpoint!).Port;
            var options = new RuvioClientOptions
            {
                Host = "127.0.0.1",
                Port = port,
                DiscoverCluster = false,
                ReconnectBaseDelay = TimeSpan.Zero,
                ReconnectMaxDelay = TimeSpan.Zero,
            };

            configure(options);
            Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(options);

            _accepted = await TestDeadlines.AcceptAsync(_listener);

            return await connecting;
        }

        public void StopListening()
        {
            _listener.Stop();
        }

        public void CloseAccepted()
        {
            _accepted?.Dispose();
            _accepted = null;
        }

        public async Task AcceptAndPong()
        {
            _accepted = await TestDeadlines.AcceptAsync(_listener);
            NetworkStream stream = _accepted.GetStream();
            var got = new byte[14];

            await TestDeadlines.ReadExactlyAsync(stream, got);
            Assert.Equal("*1\r\n$4\r\nPING\r\n", Encoding.ASCII.GetString(got));
            await stream.WriteAsync(Encoding.ASCII.GetBytes("+PONG\r\n"));
        }

        public async Task AcceptQuietly()
        {
            _accepted = await TestDeadlines.AcceptAsync(_listener);
        }

        public async Task AcceptAndNullGet()
        {
            _accepted = await TestDeadlines.AcceptAsync(_listener);
            NetworkStream stream = _accepted.GetStream();
            var got = new byte[20];

            await TestDeadlines.ReadExactlyAsync(stream, got);
            Assert.Equal("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", Encoding.ASCII.GetString(got));
            await stream.WriteAsync(Encoding.ASCII.GetBytes("$-1\r\n"));
        }

        public async Task CutAcceptedGetAsync()
        {
            NetworkStream stream = _accepted!.GetStream();
            var got = new byte[20];

            await TestDeadlines.ReadExactlyAsync(stream, got);
            await stream.WriteAsync(new byte[] { (byte)'$' });

            _accepted.Dispose();
            _accepted = null;
            _listener.Stop();
        }

        public async Task Pong()
        {
            NetworkStream stream = _accepted!.GetStream();
            var got = new byte[14];

            await TestDeadlines.ReadExactlyAsync(stream, got);
            Assert.Equal("*1\r\n$4\r\nPING\r\n", Encoding.ASCII.GetString(got));
            await stream.WriteAsync(Encoding.ASCII.GetBytes("+PONG\r\n"));
        }

        public async ValueTask DisposeAsync()
        {
            _accepted?.Dispose();
            _listener.Stop();
            await ValueTask.CompletedTask;
        }
    }
}
