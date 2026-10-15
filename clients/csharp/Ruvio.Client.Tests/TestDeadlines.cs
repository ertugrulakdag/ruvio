using System.Net.Sockets;

namespace Ruvio.Client.Tests;

/// <summary>
/// Scripted sockets must fail the test when the other side never writes.
/// A missing reply used to leave <c>ReadExactlyAsync</c> running until the
/// runner was killed.
/// </summary>
internal static class TestDeadlines
{
    public static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

    public static async Task<TcpClient> AcceptAsync(TcpListener listener)
    {
        using var timeout = new CancellationTokenSource(Bound);

        return await listener.AcceptTcpClientAsync(timeout.Token);
    }

    public static async Task ReadExactlyAsync(NetworkStream stream, Memory<byte> buffer)
    {
        using var timeout = new CancellationTokenSource(Bound);

        await stream.ReadExactlyAsync(buffer, timeout.Token);
    }
}
