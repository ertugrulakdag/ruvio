namespace Ruvio.Client;

/// <summary>
/// A Ruvio server error reply, or a RESP framing failure on the connection.
/// </summary>
/// <remarks>
/// Creates an exception. Server replies keep the connection in sync.
/// Framing failures do not.
/// </remarks>
public sealed class RuvioException(string message, bool isServerReply = false, Exception? inner = null, bool closedBeforeReply = false) : Exception(message, inner)
{

    /// <summary>
    /// True when Ruvio returned a RESP error and the next command can still run.
    /// </summary>
    public bool IsServerReply { get; } = isServerReply;

    /// <summary>
    /// True when the socket closed before any byte of this command's reply arrived.
    /// A single read can be sent again. A write cannot.
    /// </summary>
    public bool ClosedBeforeReply { get; } = closedBeforeReply;
}
