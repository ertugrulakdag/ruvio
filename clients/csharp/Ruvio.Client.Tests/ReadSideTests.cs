using System.Net;
using System.Net.Sockets;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>
/// The read side of a command socket: a caller reading its own reply, the background reader, and
/// <see cref="RuvioClientOptions.DedicatedReaderThread"/> (blocking sockets with a reader thread each).
/// </summary>
[Collection("Live")]
public class ReadSideTests
{
    [LiveFact(Timeout = 15000)]
    public async Task CommandsPipelinesAndConcurrentCallersWork()
    {
        (string host, int port) = LiveServer.Address!.Value;
        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = host,
            Port = port,
            Database = 13,
            DiscoverCluster = false,
            DedicatedReaderThread = true,
        });
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        await client.FlushDatabaseAsync();
        Assert.Equal("PONG", await client.PingAsync());
        await client.SetAsync("dedicated:a", "1");
        Assert.Equal("1", await client.GetStringAsync("dedicated:a"));

        var commands = Enumerable.Range(0, 50)
            .Select(i => (IReadOnlyList<string>)(i % 2 == 0 ? ["SET", $"dedicated:{i}", $"v{i}"] : ["GET", $"dedicated:{i - 1}"]))
            .ToArray();
        IReadOnlyList<RespValue> replies = await client.ExecuteManyAsync(commands);

        Assert.Equal(50, replies.Count);
        for (int i = 1; i < 50; i += 2)
        {
            Assert.Equal($"v{i - 1}", replies[i].AsString());
        }

        // Callers on many threads share the socket; every reply reaches its own caller.
        await Task.WhenAll(Enumerable.Range(0, 32).Select(worker => Task.Run(async () =>
        {
            for (int round = 0; round < 50; round++)
            {
                string key = $"dedicated:w{worker}:{round}";

                await client.SetAsync(key, key);
                Assert.Equal(key, await client.GetStringAsync(key));
            }
        })));

        // A server error is a reply like any other and does not break the socket.
        await client.SetAsync("dedicated:text", "x");
        await Assert.ThrowsAsync<RuvioException>(async () => await client.IncrementAsync("dedicated:text"));
        Assert.Equal("x", await client.GetStringAsync("dedicated:text"));
    }

    [Fact(Timeout = 15000)]
    public async Task ClusterPipelinesKeepCommandOrder()
    {
        string? address = Environment.GetEnvironmentVariable("RUVIO_CLUSTER_ADDR");

        if (string.IsNullOrWhiteSpace(address))
        {
            return;
        }

        string[] parts = address.Split(':');

        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = parts[0],
            Port = int.Parse(parts[1]),
            DedicatedReaderThread = true,
        });
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        Assert.True(client.IsCluster);

        var keys = Enumerable.Range(0, 64).Select(i => $"dedicated-cluster:{i}").ToArray();
        IReadOnlyList<RespValue> sets = await client.ExecuteManyAsync(
            keys.Select(key => (IReadOnlyList<string>)["SET", key, key]).ToArray());

        Assert.All(sets, reply => Assert.Equal("OK", reply.AsString()));

        IReadOnlyList<RespValue> gets = await client.ExecuteManyAsync(
            keys.Select(key => (IReadOnlyList<string>)["GET", key]).ToArray());

        Assert.Equal(keys, gets.Select(reply => reply.AsString()));
        Assert.Equal(keys, await client.GetManyAsync(keys));
    }

    [LiveFact(Timeout = 15000)]
    public async Task CancellableAndPlainCallersShareTheReadSide()
    {
        (string host, int port) = LiveServer.Address!.Value;
        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = host,
            Port = port,
            Database = 13,
            DiscoverCluster = false,
        });
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        using var cancel = new CancellationTokenSource();

        // Callers without a token may read for everyone; callers with one wait for the reader.
        await Task.WhenAll(Enumerable.Range(0, 32).Select(worker => Task.Run(async () =>
        {
            CancellationToken token = worker % 2 == 0 ? CancellationToken.None : cancel.Token;

            for (int round = 0; round < 100; round++)
            {
                string key = $"lead:{worker}:{round}";
                IReadOnlyList<RespValue> replies = await client.ExecuteManyAsync(
                    [["SET", key, key], ["GET", key]], token);

                Assert.Equal(key, replies[1].AsString());
            }
        })));
    }

    [Theory(Timeout = 15000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task APeerThatClosesFailsTheCommandAndTheNextOneReconnects(bool dedicatedReader)
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        Task server = Task.Run(async () =>
        {
            // First connection: read the command, then hang up without a reply.
            using (TcpClient first = await listener.AcceptTcpClientAsync())
            {
                await first.GetStream().ReadAsync(new byte[256]);
            }

            // Second connection: answer one PING.
            using TcpClient second = await listener.AcceptTcpClientAsync();
            NetworkStream stream = second.GetStream();

            await stream.ReadAsync(new byte[256]);
            await stream.WriteAsync("+PONG\r\n"u8.ToArray());
            await stream.ReadAsync(new byte[16]);
        });

        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = "127.0.0.1",
            Port = port,
            DiscoverCluster = false,
            DedicatedReaderThread = dedicatedReader,
            ReconnectBaseDelay = TimeSpan.Zero,
        });
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        RuvioException error = await Assert.ThrowsAsync<RuvioException>(async () => await client.PingAsync());

        Assert.True(error.ClosedBeforeReply);
        Assert.Equal("PONG", await client.PingAsync());
        await connection.DisposeAsync();
        await server;
    }
}
