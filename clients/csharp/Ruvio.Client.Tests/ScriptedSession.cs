using System.Net;
using System.Net.Sockets;
using System.Text;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

internal sealed class ScriptedSession : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly TcpClient _accepted;
    private readonly NetworkStream _stream;
    private readonly List<TcpClient> _extra = [];

    private ScriptedSession(TcpListener listener, TcpClient accepted, RuvioConnection connection, RuvioClient client)
    {
        _listener = listener;
        _accepted = accepted;
        _stream = accepted.GetStream();
        Connection = connection;
        Client = client;
    }

    public RuvioConnection Connection { get; }

    public RuvioClient Client { get; }

    public static async Task<ScriptedSession> Open(int database = 0, int maxInFlight = 256)
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);

        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint!).Port;
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = "127.0.0.1",
            Port = port,
            Database = database,
            DiscoverCluster = false,
            MaxInFlightRequests = maxInFlight,
        });

        TcpClient accepted = await TestDeadlines.AcceptAsync(listener);
        NetworkStream stream = accepted.GetStream();

        if (database != 0)
        {
            byte[] expected = RespCodec.Encode(["SELECT", database.ToString()]);
            var received = new byte[expected.Length];
            await TestDeadlines.ReadExactlyAsync(stream, received);
            Assert.Equal(expected, received);
            await stream.WriteAsync(Encoding.ASCII.GetBytes("+OK\r\n"));
        }

        RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        return new ScriptedSession(listener, accepted, connection, client);
    }

    public async Task<SubscriberPipe> AcceptDatabaseAsync()
    {
        TcpClient accepted = await TestDeadlines.AcceptAsync(_listener);
        _extra.Add(accepted);

        return new SubscriberPipe(accepted);
    }

    public async Task Expect(params string[] arguments)
    {
        byte[] expected = RespCodec.Encode(arguments);
        var got = new byte[expected.Length];

        await TestDeadlines.ReadExactlyAsync(_stream, got);
        Assert.Equal(Encoding.ASCII.GetString(expected), Encoding.ASCII.GetString(got));
    }

    public Task Reply(string resp)
    {
        return _stream.WriteAsync(Encoding.ASCII.GetBytes(resp)).AsTask();
    }

    public void CloseCommandSocket() => _accepted.Dispose();

    public async Task ExpectBytes(IReadOnlyList<ReadOnlyMemory<byte>> arguments)
    {
        byte[] expected = RespCodec.Encode(arguments);
        var got = new byte[expected.Length];

        await TestDeadlines.ReadExactlyAsync(_stream, got);
        Assert.Equal(expected, got);
    }

    public Task ReplyBytes(byte[] bytes) => _stream.WriteAsync(bytes).AsTask();

    /// <summary>Accepts the dedicated Pub/Sub socket that subscribe methods open.</summary>
    public async Task<SubscriberPipe> AcceptSubscriberAsync()
    {
        TcpClient accepted = await TestDeadlines.AcceptAsync(_listener);

        _extra.Add(accepted);

        return new SubscriberPipe(accepted);
    }

    public async ValueTask DisposeAsync()
    {
        Connection.Dispose();

        foreach (TcpClient extra in _extra)
        {
            extra.Dispose();
        }

        _accepted.Dispose();
        _listener.Stop();
        await ValueTask.CompletedTask;
    }
}

internal sealed class SubscriberPipe(TcpClient client) : IAsyncDisposable
{
    private readonly NetworkStream _stream = client.GetStream();

    public void Close()
    {
        client.Close();
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    public async Task Expect(params string[] arguments)
    {
        byte[] expected = RespCodec.Encode(arguments);
        var got = new byte[expected.Length];

        await TestDeadlines.ReadExactlyAsync(_stream, got);
        Assert.Equal(Encoding.ASCII.GetString(expected), Encoding.ASCII.GetString(got));
    }

    public Task Reply(string resp)
    {
        return _stream.WriteAsync(Encoding.ASCII.GetBytes(resp)).AsTask();
    }
}
