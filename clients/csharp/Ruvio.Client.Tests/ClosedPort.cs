using System.Net;
using System.Net.Sockets;

namespace Ruvio.Client.Tests;

/// <summary>
/// Stops a listener and keeps its port bound without listening, so connects to it are refused.
/// Test classes run in parallel: a freed port could otherwise be handed to another test's new
/// listener, and a reconnect that should fail would reach that server instead.
/// </summary>
internal static class ClosedPort
{
    /// <summary>The returned socket holds the port until it is disposed; null when the port could not be held.</summary>
    public static Socket? Stop(TcpListener listener)
    {
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;

        listener.Stop();
        var holder = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

        try
        {
            // Lets the bind share the port with the stopped listener's connections; a socket that
            // is bound but not listening refuses every connect.
            holder.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            holder.Bind(new IPEndPoint(IPAddress.Loopback, port));

            return holder;
        }

        catch (SocketException)
        {
            // The port stays free, as it was before this helper existed.
            holder.Dispose();

            return null;
        }
    }
}
